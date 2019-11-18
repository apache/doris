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

#include "exec/hash_table.hpp"

#include "codegen/codegen_anyval.h"
#include "codegen/llvm_codegen.h"

#include "exprs/expr.h"
#include "runtime/raw_value.h"
#include "runtime/string_value.hpp"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "util/doris_metrics.h"

using llvm::BasicBlock;
using llvm::Value;
using llvm::Function;
using llvm::Type;
using llvm::PointerType;
using llvm::LLVMContext;
using llvm::PHINode;

namespace doris {

const float HashTable::MAX_BUCKET_OCCUPANCY_FRACTION = 0.75f;
const char* HashTable::_s_llvm_class_name = "class.doris::HashTable";

HashTable::HashTable(const vector<ExprContext*>& build_expr_ctxs,
                     const vector<ExprContext*>& probe_expr_ctxs,
                     int num_build_tuples, bool stores_nulls, 
                     const std::vector<bool>& finds_nulls,
                     int32_t initial_seed,
                     MemTracker* mem_tracker, int64_t num_buckets) :
        _build_expr_ctxs(build_expr_ctxs),
        _probe_expr_ctxs(probe_expr_ctxs),
        _num_build_tuples(num_build_tuples),
        _stores_nulls(stores_nulls),
        _finds_nulls(finds_nulls),
        _initial_seed(initial_seed),
        _node_byte_size(sizeof(Node) + sizeof(Tuple*) * _num_build_tuples),
        _num_filled_buckets(0),
        _nodes(NULL),
        _num_nodes(0),
        _exceeded_limit(false),
        _mem_tracker(mem_tracker),
        _mem_limit_exceeded(false) {
    DCHECK(mem_tracker != NULL);
    DCHECK_EQ(_build_expr_ctxs.size(), _probe_expr_ctxs.size());

    DCHECK_EQ((num_buckets & (num_buckets - 1)), 0) << "num_buckets must be a power of 2";
    _buckets.resize(num_buckets);
    _num_buckets = num_buckets;
    _num_buckets_till_resize = MAX_BUCKET_OCCUPANCY_FRACTION * _num_buckets;
    _mem_tracker->consume(_buckets.capacity() * sizeof(Bucket));

    // Compute the layout and buffer size to store the evaluated expr results
    _results_buffer_size = Expr::compute_results_layout(_build_expr_ctxs,
                           &_expr_values_buffer_offsets, &_var_result_begin);
    _expr_values_buffer = new uint8_t[_results_buffer_size];
    memset(_expr_values_buffer, 0, sizeof(uint8_t) * _results_buffer_size);
    _expr_value_null_bits = new uint8_t[_build_expr_ctxs.size()];

    _nodes_capacity = 1024;
    _nodes = reinterpret_cast<uint8_t*>(malloc(_nodes_capacity * _node_byte_size));
    memset(_nodes, 0, _nodes_capacity * _node_byte_size);

#if 0
    if (DorisMetrics::hash_table_total_bytes() != NULL) {
        DorisMetrics::hash_table_total_bytes()->increment(_nodes_capacity * _node_byte_size);
    }
#endif

    _mem_tracker->consume(_nodes_capacity * _node_byte_size);
    if (_mem_tracker->limit_exceeded()) {
        mem_limit_exceeded(_nodes_capacity * _node_byte_size);
    }
}

HashTable::~HashTable() {
}

void HashTable::close() {
    // TODO: use tr1::array?
    delete[] _expr_values_buffer;
    delete[] _expr_value_null_bits;
    free(_nodes);
#if 0
    if (DorisMetrics::hash_table_total_bytes() != NULL) {
        DorisMetrics::hash_table_total_bytes()->increment(-_nodes_capacity * _node_byte_size);
    }
#endif
    _mem_tracker->release(_nodes_capacity * _node_byte_size);
    _mem_tracker->release(_buckets.size() * sizeof(Bucket));
}

bool HashTable::eval_row(TupleRow* row, const vector<ExprContext*>& ctxs) {
    // Put a non-zero constant in the result location for NULL.
    // We don't want(NULL, 1) to hash to the same as (0, 1).
    // This needs to be as big as the biggest primitive type since the bytes
    // get copied directly.

    // the 10 is experience value which need bigger than sizeof(Decimal)/sizeof(int64).
    // for if slot is null, we need copy the null value to all type.
    static int64_t null_value[10] = {HashUtil::FNV_SEED, HashUtil::FNV_SEED, 0};
    bool has_null = false;

    for (int i = 0; i < ctxs.size(); ++i) {
        void* loc = _expr_values_buffer + _expr_values_buffer_offsets[i];
        void* val = ctxs[i]->get_value(row);

        if (val == NULL) {
            // If the table doesn't store nulls, no reason to keep evaluating
            if (!_stores_nulls) {
                return true;
            }

            _expr_value_null_bits[i] = true;
            val = &null_value;
            has_null = true;
        } else {
            _expr_value_null_bits[i] = false;
        }

        RawValue::write(val, loc, _build_expr_ctxs[i]->root()->type(), NULL);
    }

    return has_null;
}

uint32_t HashTable::hash_variable_len_row() {
    uint32_t hash = _initial_seed;
    // Hash the non-var length portions (if there are any)
    if (_var_result_begin != 0) {
        hash = HashUtil::hash(_expr_values_buffer, _var_result_begin, hash);
    }

    for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
        // non-string and null slots are already part of expr_values_buffer
        if (_build_expr_ctxs[i]->root()->type().is_string_type()) {
            void* loc = _expr_values_buffer + _expr_values_buffer_offsets[i];

            if (_expr_value_null_bits[i]) {
                // Hash the null random seed values at 'loc'
                hash = HashUtil::hash(loc, sizeof(StringValue), hash);
            } else {
                // Hash the string
                StringValue* str = reinterpret_cast<StringValue*>(loc);
                hash = HashUtil::hash(str->ptr, str->len, hash);
            }
        } else if (_build_expr_ctxs[i]->root()->type().type == TYPE_DECIMAL) {
            void* loc = _expr_values_buffer + _expr_values_buffer_offsets[i];
            if (_expr_value_null_bits[i]) {
                // Hash the null random seed values at 'loc'
                hash = HashUtil::hash(loc, sizeof(StringValue), hash);
            } else {
                DecimalValue* decimal = reinterpret_cast<DecimalValue*>(loc);
                hash = decimal->hash(hash);
            }
        }
        
    }

    return hash;
}

bool HashTable::equals(TupleRow* build_row) {
    for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
        void* val = _build_expr_ctxs[i]->get_value(build_row);

        if (val == NULL) {
            if (!(_stores_nulls && _finds_nulls[i])) {
                return false;
            }

            if (!_expr_value_null_bits[i]) {
                return false;
            }

            continue;
        }

        void* loc = _expr_values_buffer + _expr_values_buffer_offsets[i];

        if (!RawValue::eq(loc, val, _build_expr_ctxs[i]->root()->type())) {
            return false;

        }
    }

    return true;
}

void HashTable::resize_buckets(int64_t num_buckets) {
    DCHECK_EQ((num_buckets & (num_buckets - 1)), 0) << "num_buckets must be a power of 2";

    int64_t old_num_buckets = _num_buckets;
    int64_t delta_bytes = (num_buckets - old_num_buckets) * sizeof(Bucket);
    if (!_mem_tracker->try_consume(delta_bytes)) {
        mem_limit_exceeded(delta_bytes);
        return;
    }

    _buckets.resize(num_buckets);

    // If we're doubling the number of buckets, all nodes in a particular bucket
    // either remain there, or move down to an analogous bucket in the other half.
    // In order to efficiently check which of the two buckets a node belongs in, the number
    // of buckets must be a power of 2.
    bool doubled_buckets = (num_buckets == old_num_buckets * 2);

    for (int i = 0; i < _num_buckets; ++i) {
        Bucket* bucket = &_buckets[i];
        Bucket* sister_bucket = &_buckets[i + old_num_buckets];
        Node* last_node = NULL;
        int node_idx = bucket->_node_idx;

        while (node_idx != -1) {
            Node* node = get_node(node_idx);
            int64_t next_idx = node->_next_idx;
            uint32_t hash = node->_hash;

            bool node_must_move = true;
            Bucket* move_to = NULL;

            if (doubled_buckets) {
                node_must_move = ((hash & old_num_buckets) != 0);
                move_to = sister_bucket;
            } else {
                int64_t bucket_idx = hash & (num_buckets - 1);
                node_must_move = (bucket_idx != i);
                move_to = &_buckets[bucket_idx];
            }

            if (node_must_move) {
                move_node(bucket, move_to, node_idx, node, last_node);
            } else {
                last_node = node;
            }

            node_idx = next_idx;
        }
    }

    _num_buckets = num_buckets;
    _num_buckets_till_resize = MAX_BUCKET_OCCUPANCY_FRACTION * _num_buckets;
}

void HashTable::grow_node_array() {
    int64_t old_size = _nodes_capacity * _node_byte_size;
    _nodes_capacity = _nodes_capacity + _nodes_capacity / 2;
    int64_t new_size = _nodes_capacity * _node_byte_size;

    uint8_t* new_nodes = reinterpret_cast<uint8_t*>(malloc(new_size));
    memset(new_nodes, 0, new_size);
    memcpy(new_nodes, _nodes, old_size);
    free(_nodes);
    _nodes = new_nodes; 

#if 0
    if (DorisMetrics::hash_table_total_bytes() != NULL) {
        DorisMetrics::hash_table_total_bytes()->increment(new_size - old_size);
    }
#endif

    _mem_tracker->consume(new_size - old_size);
    if (_mem_tracker->limit_exceeded()) {
        mem_limit_exceeded(new_size - old_size);
    }
}

void HashTable::mem_limit_exceeded(int64_t allocation_size) {
    _mem_limit_exceeded = true;
    _exceeded_limit = true;
    // if (_state != NULL) {
    //     _state->set_mem_limit_exceeded(_mem_tracker, allocation_size);
    // }
}

std::string HashTable::debug_string(bool skip_empty, const RowDescriptor* desc) {
    std::stringstream ss;
    ss << std::endl;

    for (int i = 0; i < _buckets.size(); ++i) {
        int64_t node_idx = _buckets[i]._node_idx;
        bool first = true;

        if (skip_empty && node_idx == -1) {
            continue;
        }

        ss << i << ": ";

        while (node_idx != -1) {
            Node* node = get_node(node_idx);

            if (!first) {
                ss << ",";
            }

            if (desc == NULL) {
                ss << node_idx << "(" << (void*)node->data() << ")";
            } else {
                ss << (void*)node->data() << " " << node->data()->to_string(*desc);
            }

            node_idx = node->_next_idx;
            first = false;
        }

        ss << std::endl;
    }

    return ss.str();
}

// Helper function to store a value into the results buffer if the expr
// evaluated to NULL.  We don't want (NULL, 1) to hash to the same as (0,1) so
// we'll pick a more random value.
static void codegen_assign_null_value(
        LlvmCodeGen* codegen, LlvmCodeGen::LlvmBuilder* builder,
        Value* dst, const TypeDescriptor& type) {
    int64_t fvn_seed = HashUtil::FNV_SEED;

    if (type.type == TYPE_CHAR || type.type == TYPE_VARCHAR) {
        Value* dst_ptr = builder->CreateStructGEP(dst, 0, "string_ptr");
        Value* dst_len = builder->CreateStructGEP(dst, 1, "string_len");
        Value* null_len = codegen->get_int_constant(TYPE_INT, fvn_seed);
        Value* null_ptr = builder->CreateIntToPtr(null_len, codegen->ptr_type());
        builder->CreateStore(null_ptr, dst_ptr);
        builder->CreateStore(null_len, dst_len);
        return;
    } else {
        Value* null_value = NULL;
        // Get a type specific representation of fvn_seed
        switch (type.type) {
        case TYPE_BOOLEAN:
            // In results, booleans are stored as 1 byte
            dst = builder->CreateBitCast(dst, codegen->ptr_type());
            null_value = codegen->get_int_constant(TYPE_TINYINT, fvn_seed);
            break;
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
            null_value = codegen->get_int_constant(type.type, fvn_seed);
            break;
        case TYPE_FLOAT: {
            // Don't care about the value, just the bit pattern
            float fvn_seed_float = *reinterpret_cast<float*>(&fvn_seed);
            null_value = llvm::ConstantFP::get(
                codegen->context(), llvm::APFloat(fvn_seed_float));
            break;
        }
        case TYPE_DOUBLE: {
            // Don't care about the value, just the bit pattern
            double fvn_seed_double = *reinterpret_cast<double*>(&fvn_seed);
            null_value = llvm::ConstantFP::get(
                codegen->context(), llvm::APFloat(fvn_seed_double));
            break;
        }
        default:
            DCHECK(false);
        }
        builder->CreateStore(null_value, dst);
    }
}

// Codegen for evaluating a tuple row over either _build_expr_ctxs or _probe_expr_ctxs.
// For the case where we are joining on a single int, the IR looks like
// define i1 @EvaBuildRow(%"class.impala::HashTable"* %this_ptr,
//                        %"class.impala::TupleRow"* %row) {
// entry:
//   %null_ptr = alloca i1
//   %0 = bitcast %"class.doris::TupleRow"* %row to i8**
//   %eval = call i32 @SlotRef(i8** %0, i8* null, i1* %null_ptr)
//   %1 = load i1* %null_ptr
//   br i1 %1, label %null, label %not_null
//
// null:                                             ; preds = %entry
//   ret i1 true
//
// not_null:                                         ; preds = %entry
//   store i32 %eval, i32* inttoptr (i64 46146336 to i32*)
//   br label %continue
//
// continue:                                         ; preds = %not_null
//   %2 = zext i1 %1 to i8
//   store i8 %2, i8* inttoptr (i64 46146248 to i8*)
//   ret i1 false
// }
// For each expr, we create 3 code blocks.  The null, not null and continue blocks.
// Both the null and not null branch into the continue block.  The continue block
// becomes the start of the next block for codegen (either the next expr or just the
// end of the function).
Function* HashTable::codegen_eval_tuple_row(RuntimeState* state, bool build) {
    // TODO: codegen_assign_null_value() can't handle TYPE_TIMESTAMP or TYPE_DECIMAL yet
    const std::vector<ExprContext*>& ctxs = build ? _build_expr_ctxs : _probe_expr_ctxs;
    for (int i = 0; i < ctxs.size(); ++i) {
        PrimitiveType type = ctxs[i]->root()->type().type;
        if (type == TYPE_DATE || type == TYPE_DATETIME
                || type == TYPE_DECIMAL || type == TYPE_CHAR || type == TYPE_DECIMALV2) {
            return NULL;
        }
    }

    LlvmCodeGen* codegen = NULL;
    if (!state->get_codegen(&codegen).ok()) {
        return NULL;
    }

    // Get types to generate function prototype
    Type* tuple_row_type = codegen->get_type(TupleRow::_s_llvm_class_name);
    DCHECK(tuple_row_type != NULL);
    PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);

    Type* this_type = codegen->get_type(HashTable::_s_llvm_class_name);
    DCHECK(this_type != NULL);
    PointerType* this_ptr_type = PointerType::get(this_type, 0);

    LlvmCodeGen::FnPrototype prototype(
        codegen, build ? "eval_build_row" : "eval_probe_row", codegen->get_type(TYPE_BOOLEAN));
    prototype.add_argument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
    prototype.add_argument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

    LLVMContext& context = codegen->context();
    LlvmCodeGen::LlvmBuilder builder(context);
    Value* args[2];
    Function* fn = prototype.generate_prototype(&builder, args);

    Value* row = args[1];
    Value* has_null = codegen->false_value();

    // Aggregation with no grouping exprs also use the hash table interface for
    // code simplicity.  In that case, there are no build exprs.
    if (!_build_expr_ctxs.empty()) {
        const std::vector<ExprContext*>& ctxs = build ? _build_expr_ctxs : _probe_expr_ctxs;
        for (int i = 0; i < ctxs.size(); ++i) {
            // TODO: refactor this to somewhere else?  This is not hash table specific
            // except for the null handling bit and would be used for anyone that needs
            // to materialize a vector of exprs
            // Convert result buffer to llvm ptr type
            void* loc = _expr_values_buffer + _expr_values_buffer_offsets[i];
            Value* llvm_loc = codegen->cast_ptr_to_llvm_ptr(
                codegen->get_ptr_type(ctxs[i]->root()->type()), loc);

            BasicBlock* null_block = BasicBlock::Create(context, "null", fn);
            BasicBlock* not_null_block = BasicBlock::Create(context, "not_null", fn);
            BasicBlock* continue_block = BasicBlock::Create(context, "continue", fn);

            // Call expr
            Function* expr_fn = NULL;
            Status status = ctxs[i]->root()->get_codegend_compute_fn(state, &expr_fn);
            if (!status.ok()) {
                std::stringstream ss;
                ss << "Problem with codegen: " << status.get_error_msg();
                // TODO(zc )
                // state->LogError(ErrorMsg(TErrorCode::GENERAL, ss.str()));
                fn->eraseFromParent(); // deletes function
                return NULL;
            }

            Value* ctx_arg = codegen->cast_ptr_to_llvm_ptr(
                codegen->get_ptr_type(ExprContext::_s_llvm_class_name), ctxs[i]);
            Value* expr_fn_args[] = { ctx_arg, row };
            CodegenAnyVal result = CodegenAnyVal::create_call_wrapped(
                codegen, &builder, ctxs[i]->root()->type(),
                expr_fn, expr_fn_args, "result", NULL);
            Value* is_null = result.get_is_null();

            // Set null-byte result
            Value* null_byte = builder.CreateZExt(is_null, codegen->get_type(TYPE_TINYINT));
            uint8_t* null_byte_loc = &_expr_value_null_bits[i];
            Value* llvm_null_byte_loc =
                codegen->cast_ptr_to_llvm_ptr(codegen->ptr_type(), null_byte_loc);
            builder.CreateStore(null_byte, llvm_null_byte_loc);

            builder.CreateCondBr(is_null, null_block, not_null_block);

            // Null block
            builder.SetInsertPoint(null_block);
            if (!_stores_nulls) {
                // hash table doesn't store nulls, no reason to keep evaluating exprs
                builder.CreateRet(codegen->true_value());
            } else {
                codegen_assign_null_value(codegen, &builder, llvm_loc, ctxs[i]->root()->type());
                has_null = codegen->true_value();
                builder.CreateBr(continue_block);
            }

            // Not null block
            builder.SetInsertPoint(not_null_block);
            result.to_native_ptr(llvm_loc);
            builder.CreateBr(continue_block);

            builder.SetInsertPoint(continue_block);
        }
    }
    builder.CreateRet(has_null);

    return codegen->finalize_function(fn);
}

// Codegen for hashing the current row.  In the case with both string and non-string data
// (group by int_col, string_col), the IR looks like:
// define i32 @hash_current_row(%"class.impala::HashTable"* %this_ptr) {
// entry:
//   %0 = call i32 @IrCrcHash(i8* inttoptr (i64 51107808 to i8*), i32 16, i32 0)
//   %1 = load i8* inttoptr (i64 29500112 to i8*)
//   %2 = icmp ne i8 %1, 0
//   br i1 %2, label %null, label %not_null
//
// null:                                             ; preds = %entry
//   %3 = call i32 @IrCrcHash(i8* inttoptr (i64 51107824 to i8*), i32 16, i32 %0)
//   br label %continue
//
// not_null:                                         ; preds = %entry
//   %4 = load i8** getelementptr inbounds (
//        %"struct.impala::StringValue"* inttoptr
//          (i64 51107824 to %"struct.impala::StringValue"*), i32 0, i32 0)
//   %5 = load i32* getelementptr inbounds (
//        %"struct.impala::StringValue"* inttoptr
//          (i64 51107824 to %"struct.impala::StringValue"*), i32 0, i32 1)
//   %6 = call i32 @IrCrcHash(i8* %4, i32 %5, i32 %0)
//   br label %continue
//
// continue:                                         ; preds = %not_null, %null
//   %7 = phi i32 [ %6, %not_null ], [ %3, %null ]
//   ret i32 %7
// }
// TODO: can this be cross-compiled?
Function* HashTable::codegen_hash_current_row(RuntimeState* state) {
    for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
        // Disable codegen for CHAR
        if (_build_expr_ctxs[i]->root()->type().type == TYPE_CHAR) {
            return NULL;
        }
    }

    LlvmCodeGen* codegen = NULL;
    if (!state->get_codegen(&codegen).ok()) {
        return NULL;
    }

    // Get types to generate function prototype
    Type* this_type = codegen->get_type(HashTable::_s_llvm_class_name);
    DCHECK(this_type != NULL);
    PointerType* this_ptr_type = PointerType::get(this_type, 0);

    LlvmCodeGen::FnPrototype prototype(codegen, "hash_current_row", codegen->get_type(TYPE_INT));
    prototype.add_argument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));

    LLVMContext& context = codegen->context();
    LlvmCodeGen::LlvmBuilder builder(context);
    Value* this_arg = NULL;
    Function* fn = prototype.generate_prototype(&builder, &this_arg);

    Value* hash_result = codegen->get_int_constant(TYPE_INT, _initial_seed);
    Value* data = codegen->cast_ptr_to_llvm_ptr(codegen->ptr_type(), _expr_values_buffer);
    if (_var_result_begin == -1) {
        // No variable length slots, just hash what is in '_expr_values_buffer'
        if (_results_buffer_size > 0) {
            Function* hash_fn = codegen->get_hash_function(_results_buffer_size);
            Value* len = codegen->get_int_constant(TYPE_INT, _results_buffer_size);
            hash_result = builder.CreateCall3(hash_fn, data, len, hash_result);
        }
    } else {
        if (_var_result_begin > 0) {
            Function* hash_fn = codegen->get_hash_function(_var_result_begin);
            Value* len = codegen->get_int_constant(TYPE_INT, _var_result_begin);
            hash_result = builder.CreateCall3(hash_fn, data, len, hash_result);
        }

        // Hash string slots
        for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
            if (_build_expr_ctxs[i]->root()->type().type != TYPE_CHAR
                && _build_expr_ctxs[i]->root()->type().type != TYPE_VARCHAR) {
                continue;
            }

            BasicBlock* null_block = NULL;
            BasicBlock* not_null_block = NULL;
            BasicBlock* continue_block = NULL;
            Value* str_null_result = NULL;

            void* loc = _expr_values_buffer + _expr_values_buffer_offsets[i];

            // If the hash table stores nulls, we need to check if the stringval
            // evaluated to NULL
            if (_stores_nulls) {
                null_block = BasicBlock::Create(context, "null", fn);
                not_null_block = BasicBlock::Create(context, "not_null", fn);
                continue_block = BasicBlock::Create(context, "continue", fn);

                uint8_t* null_byte_loc = &_expr_value_null_bits[i];
                Value* llvm_null_byte_loc =
                    codegen->cast_ptr_to_llvm_ptr(codegen->ptr_type(), null_byte_loc);
                Value* null_byte = builder.CreateLoad(llvm_null_byte_loc);
                Value* is_null = builder.CreateICmpNE(
                    null_byte, codegen->get_int_constant(TYPE_TINYINT, 0));
                builder.CreateCondBr(is_null, null_block, not_null_block);

                // For null, we just want to call the hash function on the portion of
                // the data
                builder.SetInsertPoint(null_block);
                Function* null_hash_fn = codegen->get_hash_function(sizeof(StringValue));
                Value* llvm_loc = codegen->cast_ptr_to_llvm_ptr(codegen->ptr_type(), loc);
                Value* len = codegen->get_int_constant(TYPE_INT, sizeof(StringValue));
                str_null_result = builder.CreateCall3(null_hash_fn, llvm_loc, len, hash_result);
                builder.CreateBr(continue_block);

                builder.SetInsertPoint(not_null_block);
            }

            // Convert _expr_values_buffer loc to llvm value
            Value* str_val = codegen->cast_ptr_to_llvm_ptr(
                codegen->get_ptr_type(TYPE_VARCHAR), loc);

            Value* ptr = builder.CreateStructGEP(str_val, 0, "ptr");
            Value* len = builder.CreateStructGEP(str_val, 1, "len");
            ptr = builder.CreateLoad(ptr);
            len = builder.CreateLoad(len);

            // Call hash(ptr, len, hash_result);
            Function* general_hash_fn = codegen->get_hash_function();
            Value* string_hash_result =
                builder.CreateCall3(general_hash_fn, ptr, len, hash_result);

            if (_stores_nulls) {
                builder.CreateBr(continue_block);
                builder.SetInsertPoint(continue_block);
                // Use phi node to reconcile that we could have come from the string-null
                // path and string not null paths.
                PHINode* phi_node = builder.CreatePHI(codegen->get_type(TYPE_INT), 2);
                phi_node->addIncoming(string_hash_result, not_null_block);
                phi_node->addIncoming(str_null_result, null_block);
                hash_result = phi_node;
            } else {
                hash_result = string_hash_result;
            }
        }
    }

    builder.CreateRet(hash_result);
    return codegen->finalize_function(fn);
}

// Codegen for HashTable::Equals.  For a hash table with two exprs (string,int), the
// IR looks like:
//
// define i1 @Equals(%"class.impala::OldHashTable"* %this_ptr,
//                   %"class.impala::TupleRow"* %row) {
// entry:
//   %result = call i64 @get_slot_ref(%"class.impala::ExprContext"* inttoptr
//                                  (i64 146381856 to %"class.impala::ExprContext"*),
//                                  %"class.impala::TupleRow"* %row)
//   %0 = trunc i64 %result to i1
//   br i1 %0, label %null, label %not_null
//
// false_block:                            ; preds = %not_null2, %null1, %not_null, %null
//   ret i1 false
//
// null:                                             ; preds = %entry
//   br i1 false, label %continue, label %false_block
//
// not_null:                                         ; preds = %entry
//   %1 = load i32* inttoptr (i64 104774368 to i32*)
//   %2 = ashr i64 %result, 32
//   %3 = trunc i64 %2 to i32
//   %cmp_raw = icmp eq i32 %3, %1
//   br i1 %cmp_raw, label %continue, label %false_block
//
// continue:                                         ; preds = %not_null, %null
//   %result4 = call { i64, i8* } @get_slot_ref(
//       %"class.impala::ExprContext"* inttoptr
//       (i64 146381696 to %"class.impala::ExprContext"*),
//       %"class.impala::TupleRow"* %row)
//   %4 = extractvalue { i64, i8* } %result4, 0
//   %5 = trunc i64 %4 to i1
//   br i1 %5, label %null1, label %not_null2
//
// null1:                                            ; preds = %continue
//   br i1 false, label %continue3, label %false_block
//
// not_null2:                                        ; preds = %continue
//   %6 = extractvalue { i64, i8* } %result4, 0
//   %7 = ashr i64 %6, 32
//   %8 = trunc i64 %7 to i32
//   %result5 = extractvalue { i64, i8* } %result4, 1
//   %cmp_raw6 = call i1 @_Z11StringValEQPciPKN6impala11StringValueE(
//       i8* %result5, i32 %8, %"struct.impala::StringValue"* inttoptr
//       (i64 104774384 to %"struct.impala::StringValue"*))
//   br i1 %cmp_raw6, label %continue3, label %false_block
//
// continue3:                                        ; preds = %not_null2, %null1
//   ret i1 true
// }
Function* HashTable::codegen_equals(RuntimeState* state) {
    for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
        // Disable codegen for CHAR
        if (_build_expr_ctxs[i]->root()->type().type == TYPE_CHAR) {
            return NULL;
        }
    }

    LlvmCodeGen* codegen = NULL;
    if (!state->get_codegen(&codegen).ok()) {
        return NULL;
    }
    // Get types to generate function prototype
    Type* tuple_row_type = codegen->get_type(TupleRow::_s_llvm_class_name);
    DCHECK(tuple_row_type != NULL);
    PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);

    Type* this_type = codegen->get_type(HashTable::_s_llvm_class_name);
    DCHECK(this_type != NULL);
    PointerType* this_ptr_type = PointerType::get(this_type, 0);

    LlvmCodeGen::FnPrototype prototype(codegen, "equals", codegen->get_type(TYPE_BOOLEAN));
    prototype.add_argument(LlvmCodeGen::NamedVariable("this_ptr", this_ptr_type));
    prototype.add_argument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

    LLVMContext& context = codegen->context();
    LlvmCodeGen::LlvmBuilder builder(context);
    Value* args[2];
    Function* fn = prototype.generate_prototype(&builder, args);
    Value* row = args[1];

    if (!_build_expr_ctxs.empty()) {
        BasicBlock* false_block = BasicBlock::Create(context, "false_block", fn);

        for (int i = 0; i < _build_expr_ctxs.size(); ++i) {
            BasicBlock* null_block = BasicBlock::Create(context, "null", fn);
            BasicBlock* not_null_block = BasicBlock::Create(context, "not_null", fn);
            BasicBlock* continue_block = BasicBlock::Create(context, "continue", fn);

            // call GetValue on build_exprs[i]
            Function* expr_fn = NULL;
            Status status = _build_expr_ctxs[i]->root()->get_codegend_compute_fn(state, &expr_fn);
            if (!status.ok()) {
                std::stringstream ss;
                ss << "Problem with codegen: " << status.get_error_msg();
                // TODO(zc)
                // state->LogError(ErrorMsg(TErrorCode::GENERAL, ss.str()));
                fn->eraseFromParent(); // deletes function
                return NULL;
            }

            Value* ctx_arg = codegen->cast_ptr_to_llvm_ptr(
                codegen->get_ptr_type(ExprContext::_s_llvm_class_name), _build_expr_ctxs[i]);
            Value* expr_fn_args[] = { ctx_arg, row };
            CodegenAnyVal result = CodegenAnyVal::create_call_wrapped(
                codegen, &builder, _build_expr_ctxs[i]->root()->type(),
                expr_fn, expr_fn_args, "result", NULL);
            Value* is_null = result.get_is_null();

            // Determine if probe is null (i.e. _expr_value_null_bits[i] == true). In
            // the case where the hash table does not store nulls, this is always false.
            Value* probe_is_null = codegen->false_value();
            uint8_t* null_byte_loc = &_expr_value_null_bits[i];
            if (_stores_nulls) {
                Value* llvm_null_byte_loc =
                    codegen->cast_ptr_to_llvm_ptr(codegen->ptr_type(), null_byte_loc);
                Value* null_byte = builder.CreateLoad(llvm_null_byte_loc);
                probe_is_null = builder.CreateICmpNE(
                    null_byte, codegen->get_int_constant(TYPE_TINYINT, 0));
            }

            // Get llvm value for probe_val from '_expr_values_buffer'
            void* loc = _expr_values_buffer + _expr_values_buffer_offsets[i];
            Value* probe_val = codegen->cast_ptr_to_llvm_ptr(
                codegen->get_ptr_type(_build_expr_ctxs[i]->root()->type()), loc);

            // Branch for GetValue() returning NULL
            builder.CreateCondBr(is_null, null_block, not_null_block);

            // Null block
            builder.SetInsertPoint(null_block);
            builder.CreateCondBr(probe_is_null, continue_block, false_block);

            // Not-null block
            builder.SetInsertPoint(not_null_block);
            if (_stores_nulls) {
                BasicBlock* cmp_block = BasicBlock::Create(context, "cmp", fn);
                // First need to compare that probe expr[i] is not null
                builder.CreateCondBr(probe_is_null, false_block, cmp_block);
                builder.SetInsertPoint(cmp_block);
            }
            // Check result == probe_val
            Value* is_equal = result.eq_to_native_ptr(probe_val);
            builder.CreateCondBr(is_equal, continue_block, false_block);

            builder.SetInsertPoint(continue_block);
        }
        builder.CreateRet(codegen->true_value());

        builder.SetInsertPoint(false_block);
        builder.CreateRet(codegen->false_value());
    } else {
        builder.CreateRet(codegen->true_value());
    }

    return codegen->finalize_function(fn);
}

}
