#include "exprs/lua_fn_call.h"

#include "exprs/anyval_util.h"
#include "exprs/expr_context.h"
#include "runtime/runtime_state.h"
#include "runtime/user_function_cache.h"
#include "util/defer_op.h"

namespace doris {

LUAFnCall::LUAFnCall(const TExprNode& node)
        : Expr(node), _cache_entry(nullptr), _state(nullptr), _fn_context_index(-1) {
    DCHECK_EQ(_fn.binary_type, TFunctionBinaryType::LUA);
}

LUAFnCall::~LUAFnCall() {}

Status LUAFnCall::prepare(RuntimeState* state, const RowDescriptor& desc, ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, desc, context));
    DCHECK(!_fn.scalar_fn.symbol.empty());

    FunctionContext::TypeDesc return_type = AnyValUtil::column_type_to_type_desc(_type);
    std::vector<FunctionContext::TypeDesc> arg_types;
    bool char_arg = false;
    for (int i = 0; i < _children.size(); ++i) {
        arg_types.push_back(AnyValUtil::column_type_to_type_desc(_children[i]->type()));
        char_arg = char_arg || (_children[i]->type().type == TYPE_CHAR);
    }
    _fn_context_index = context->register_func(state, return_type, arg_types, 0);

    // init state from LUA
    _state = luaL_newstate();
    if (state == nullptr) {
        return Status::InternalError("lua env init error");
    }
    // _fn.scalar_fn.symbol
    _lua_function_symbol = _fn.scalar_fn.symbol;
    return Status::OK();
}

Status LUAFnCall::open(RuntimeState* state, ExprContext* ctx,
                       FunctionContext::FunctionStateScope scope) {
    RETURN_IF_ERROR(Expr::open(state, ctx, scope));
    // TODO pass function context to lua interpret
    std::string lua_path;
    RETURN_IF_ERROR(UserFunctionCache::instance()->acquire_lua_function(
            _fn.id, _fn.hdfs_location, _fn.checksum, &_cache_entry, &lua_path));
    luaL_openlibs(_state);
    int st = luaL_loadfile(_state, lua_path.c_str());
    if (st != 0) {
        return Status::InternalError("load lua script fail");
    }
    st = lua_pcall(_state, 0, 0, 0);
    if (st != 0) {
        return Status::InternalError("execute lua script fail");
    }
    st = lua_getglobal(_state, _lua_function_symbol.c_str());
    if (!lua_isfunction(_state, -1)) {
        return Status::InternalError("symbol is not a function");
    }
    lua_settop(_state, 0);
    return Status::OK();
}

void LUAFnCall::close(RuntimeState* state, ExprContext* context,
                      FunctionContext::FunctionStateScope scope) {
    Expr::close(state, context, scope);
    lua_close(_state);
}

int LUAFnCall::eval_children(ExprContext* context, TupleRow* row) {
    lua_getglobal(_state, _lua_function_symbol.c_str());

    for (int i = 0; i < _children.size(); ++i) {
        void* src_slot = context->get_value(_children[i], row);
        if (src_slot == nullptr) {
            lua_pushnil(_state);
            continue;
        }
        switch (_children[i]->type().type) {
        case TYPE_BOOLEAN: {
            lua_pushinteger(_state, *(bool*)src_slot);
            break;
        }
        case TYPE_TINYINT: {
            lua_pushinteger(_state, *(int8_t*)src_slot);
            break;
        }
        case TYPE_SMALLINT: {
            lua_pushinteger(_state, *(int16_t*)src_slot);
            break;
        }
        case TYPE_INT: {
            lua_pushinteger(_state, *(int*)src_slot);
            break;
        }
        case TYPE_BIGINT: {
            lua_pushinteger(_state, *(int64_t*)src_slot);
            break;
        }
        case TYPE_DOUBLE: {
            lua_pushnumber(_state, *(int*)src_slot);
            break;
        }
        case TYPE_FLOAT: {
            lua_pushnumber(_state, *(int*)src_slot);
            break;
        }
        case TYPE_VARCHAR:
        case TYPE_CHAR: {
            StringValue value = *reinterpret_cast<StringValue*>(src_slot);
            lua_pushlstring(_state, value.ptr, value.len);
            break;
        }
        default: {
            lua_pushnil(_state);
            DCHECK(false);
            break;
        }
        }
    }
    int st = lua_pcall(_state, _children.size(), 1, 0);
    if (st != 0) {
        const char* errmsg = lua_tostring(_state, -1);
        FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
        fn_ctx->set_error(errmsg);
    }
    return st;
}

template <typename T>
T LUAFnCall::_get_value_from_lua(ExprContext* context, TupleRow* row) {
    T res_val;
    int st = eval_children(context, row);
    if (st == 0 && lua_isinteger(_state, -1)) {
        res_val.val = lua_tointeger(_state, -1);
    } else {
        res_val.is_null = true;
    }
    lua_settop(_state, 0);
    return res_val;
}

doris_udf::IntVal LUAFnCall::get_int_val(ExprContext* context, TupleRow* row) {
    return _get_value_from_lua<IntVal>(context, row);
}

doris_udf::BooleanVal LUAFnCall::get_boolean_val(ExprContext* context, TupleRow* row) {
    return _get_value_from_lua<BooleanVal>(context, row);
}

doris_udf::TinyIntVal LUAFnCall::get_tiny_int_val(ExprContext* context, TupleRow* row) {
    return _get_value_from_lua<TinyIntVal>(context, row);
}

doris_udf::SmallIntVal LUAFnCall::get_small_int_val(ExprContext* context, TupleRow* row) {
    return _get_value_from_lua<SmallIntVal>(context, row);
}

doris_udf::BigIntVal LUAFnCall::get_big_int_val(ExprContext* context, TupleRow* row) {
    return _get_value_from_lua<BigIntVal>(context, row);
}

doris_udf::FloatVal LUAFnCall::get_float_val(ExprContext* context, TupleRow* row) {
    FloatVal res_val;
    int st = eval_children(context, row);
    if (st == 0 && lua_isnumber(_state, -1)) {
        res_val.val = lua_isnumber(_state, -1);
    } else {
        res_val.is_null = true;
    }
    lua_settop(_state, 0);
    return res_val;
}

doris_udf::DoubleVal LUAFnCall::get_double_val(ExprContext* context, TupleRow* row) {
    DoubleVal res_val;
    int st = eval_children(context, row);
    if (st == 0 && lua_isnumber(_state, -1)) {
        res_val.val = lua_isnumber(_state, -1);
    } else {
        res_val.is_null = true;
    }
    lua_settop(_state, 0);
    return res_val;
}

doris_udf::StringVal LUAFnCall::get_string_val(ExprContext* context, TupleRow* row) {
    StringVal res_val;
    int st = eval_children(context, row);
    FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
    if (st == 0 && lua_isstring(_state, -1)) {
        size_t len = 0;
        const char* ptr = lua_tolstring(_state, -1, &len);
        StringVal val(fn_ctx, len);
        res_val = val.copy_from(fn_ctx, reinterpret_cast<const uint8_t*>(ptr), len);
    } else {
        res_val.is_null = true;
    }
    lua_settop(this->_state, 0);
    return res_val;
}

} // namespace doris