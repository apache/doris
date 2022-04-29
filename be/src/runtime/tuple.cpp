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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/tuple.cpp
// and modified by Doris

#include "runtime/tuple.h"

#include <functional>
#include <iostream>
#include <vector>

#include "common/utils.h"
#include "exprs/expr_context.h"
#include "runtime/collection_value.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/raw_value.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/mem_util.hpp"

namespace doris {

static void deep_copy_collection_slots(Tuple* shallow_copied_tuple, const TupleDescriptor& desc,
                                       const GenMemFootprintFunc& gen_mem_footprint,
                                       bool convert_ptrs);

int64_t Tuple::total_byte_size(const TupleDescriptor& desc) const {
    int64_t result = desc.byte_size();
    if (!desc.has_varlen_slots()) {
        return result;
    }
    result += varlen_byte_size(desc);
    return result;
}

int64_t Tuple::varlen_byte_size(const TupleDescriptor& desc) const {
    int64_t result = 0;
    std::vector<SlotDescriptor*>::const_iterator slot = desc.string_slots().begin();
    for (; slot != desc.string_slots().end(); ++slot) {
        DCHECK((*slot)->type().is_string_type());
        if (is_null((*slot)->null_indicator_offset())) {
            continue;
        }
        const StringValue* string_val = get_string_slot((*slot)->tuple_offset());
        result += string_val->len;
    }

    return result;
}

Tuple* Tuple::deep_copy(const TupleDescriptor& desc, MemPool* pool, bool convert_ptrs) {
    Tuple* result = (Tuple*)(pool->allocate(desc.byte_size()));
    deep_copy(result, desc, pool, convert_ptrs);
    return result;
}

void Tuple::deep_copy(Tuple* dst, const TupleDescriptor& desc, MemPool* pool, bool convert_ptrs) {
    memory_copy(dst, this, desc.byte_size());

    // allocate in the same pool and then copy all non-null string slots
    for (auto string_slot : desc.string_slots()) {
        DCHECK(string_slot->type().is_string_type());
        StringValue* string_v = dst->get_string_slot(string_slot->tuple_offset());
        if (!dst->is_null(string_slot->null_indicator_offset())) {
            if (string_v->len != 0) {
                int64_t offset = pool->total_allocated_bytes();
                char* string_copy = (char*)(pool->allocate(string_v->len));
                memory_copy(string_copy, string_v->ptr, string_v->len);
                string_v->ptr = (convert_ptrs ? convert_to<char*>(offset) : string_copy);
            }
        } else {
            string_v->ptr = nullptr;
            string_v->len = 0;
        }
    }

    // copy collection slot
    deep_copy_collection_slots(
            dst, desc,
            [pool](int size) -> MemFootprint {
                int64_t offset = pool->total_allocated_bytes();
                uint8_t* data = pool->allocate(size);
                return {offset, data};
            },
            convert_ptrs);
}

// Deep copy collection slots.
// NOTICE: The Tuple* shallow_copied_tuple must be initialized by calling memcpy function first (
// copy data from origin tuple).
static void deep_copy_collection_slots(Tuple* shallow_copied_tuple, const TupleDescriptor& desc,
                                       const GenMemFootprintFunc& gen_mem_footprint,
                                       bool convert_ptrs) {
    for (auto slot_desc : desc.collection_slots()) {
        DCHECK(slot_desc->type().is_collection_type());
        if (shallow_copied_tuple->is_null(slot_desc->null_indicator_offset())) {
            continue;
        }

        // copy collection item
        CollectionValue* cv = shallow_copied_tuple->get_collection_slot(slot_desc->tuple_offset());
        CollectionValue::deep_copy_collection(cv, slot_desc->type().children[0], gen_mem_footprint,
                                              convert_ptrs);
    }
}

Tuple* Tuple::dcopy_with_new(const TupleDescriptor& desc, MemPool* pool, int64_t* bytes) {
    Tuple* result = (Tuple*)(pool->allocate(desc.byte_size()));
    *bytes = dcopy_with_new(result, desc);
    return result;
}

int64_t Tuple::dcopy_with_new(Tuple* dst, const TupleDescriptor& desc) {
    memory_copy(dst, this, desc.byte_size());

    int64_t bytes = 0;
    // allocate in the same pool and then copy all non-null string slots
    for (auto slot : desc.string_slots()) {
        DCHECK(slot->type().is_string_type());

        if (!dst->is_null(slot->null_indicator_offset())) {
            StringValue* string_v = dst->get_string_slot(slot->tuple_offset());
            bytes += string_v->len;
            if (string_v->len != 0) {
                char* string_copy = new char[string_v->len];
                memory_copy(string_copy, string_v->ptr, string_v->len);
                string_v->ptr = string_copy;
            } else {
                string_v->ptr = nullptr;
            }
        }
    }
    return bytes;
}

int64_t Tuple::release_string(const TupleDescriptor& desc) {
    int64_t bytes = 0;
    for (auto slot : desc.string_slots()) {
        if (!is_null(slot->null_indicator_offset())) {
            StringValue* string_v = get_string_slot(slot->tuple_offset());
            delete[] string_v->ptr;
            string_v->ptr = nullptr;
            bytes += string_v->len;
        }
    }
    return bytes;
}

void Tuple::deep_copy(const TupleDescriptor& desc, char** data, int64_t* offset,
                      bool convert_ptrs) {
    Tuple* dst = (Tuple*)(*data);
    memory_copy(dst, this, desc.byte_size());
    *data += desc.byte_size();
    *offset += desc.byte_size();

    for (auto slot_desc : desc.string_slots()) {
        DCHECK(slot_desc->type().is_string_type());
        StringValue* string_v = dst->get_string_slot(slot_desc->tuple_offset());
        if (!dst->is_null(slot_desc->null_indicator_offset())) {
            memory_copy(*data, string_v->ptr, string_v->len);
            string_v->ptr = (convert_ptrs ? convert_to<char*>(*offset) : *data);
            *data += string_v->len;
            *offset += string_v->len;
        } else {
            string_v->ptr = (convert_ptrs ? convert_to<char*>(*offset) : *data);
            string_v->len = 0;
        }
    }

    // copy collection slots
    deep_copy_collection_slots(
            dst, desc,
            [offset, data](int size) -> MemFootprint {
                MemFootprint footprint = {*offset, reinterpret_cast<uint8_t*>(*data)};
                *offset += size;
                *data += size;
                return footprint;
            },
            convert_ptrs);
}

template <bool collect_string_vals>
void Tuple::materialize_exprs(TupleRow* row, const TupleDescriptor& desc,
                              const std::vector<ExprContext*>& materialize_expr_ctxs, MemPool* pool,
                              std::vector<StringValue*>* non_null_var_len_values,
                              int* total_var_len) {
    if (collect_string_vals) {
        non_null_var_len_values->clear();
        *total_var_len = 0;
    }
    memset(this, 0, desc.num_null_bytes());
    // Evaluate the output_slot_exprs and place the results in the tuples.
    int mat_expr_index = 0;
    auto& slots = desc.slots();
    for (int i = 0; i < slots.size(); ++i) {
        SlotDescriptor* slot_desc = slots[i];
        if (!slot_desc->is_materialized()) {
            continue;
        }
        // The FE ensures we don't get any TYPE_NULL expressions by picking an arbitrary type
        // when necessary, but does not do this for slot descs.
        // TODO: revisit this logic in the FE
        PrimitiveType slot_type = slot_desc->type().type;
        PrimitiveType expr_type = materialize_expr_ctxs[mat_expr_index]->root()->type().type;
        if (slot_type == TYPE_CHAR || slot_type == TYPE_VARCHAR || slot_type == TYPE_HLL ||
            slot_type == TYPE_STRING) {
            DCHECK(expr_type == TYPE_CHAR || expr_type == TYPE_VARCHAR || expr_type == TYPE_HLL ||
                   expr_type == TYPE_STRING);
        } else if (slot_type == TYPE_DATE || slot_type == TYPE_DATETIME) {
            DCHECK(expr_type == TYPE_DATE || expr_type == TYPE_DATETIME);
        } else if (slot_type == TYPE_ARRAY) {
            DCHECK(expr_type == TYPE_ARRAY);
        } else {
            DCHECK(slot_type == TYPE_NULL || slot_type == expr_type);
        }
        void* src = materialize_expr_ctxs[mat_expr_index]->get_value(row);
        if (src != nullptr) {
            void* dst = get_slot(slot_desc->tuple_offset());
            RawValue::write(src, dst, slot_desc->type(), pool);
            if (collect_string_vals) {
                if (slot_desc->type().is_string_type()) {
                    StringValue* string_val = convert_to<StringValue*>(dst);
                    non_null_var_len_values->push_back(string_val);
                    *total_var_len += string_val->len;
                }
            }
        } else {
            set_null(slot_desc->null_indicator_offset());
        }
        ++mat_expr_index;
    }

    DCHECK_EQ(mat_expr_index, materialize_expr_ctxs.size());
}

template void Tuple::materialize_exprs<false>(
        TupleRow* row, const TupleDescriptor& desc,
        const std::vector<ExprContext*>& materialize_expr_ctxs, MemPool* pool,
        std::vector<StringValue*>* non_null_var_values, int* total_var_len);

template void Tuple::materialize_exprs<true>(TupleRow* row, const TupleDescriptor& desc,
                                             const std::vector<ExprContext*>& materialize_expr_ctxs,
                                             MemPool* pool,
                                             std::vector<StringValue*>* non_null_var_values,
                                             int* total_var_len);

std::string Tuple::to_string(const TupleDescriptor& d) const {
    std::stringstream out;
    out << "(";

    bool first_value = true;
    for (auto slot : d.slots()) {
        if (!slot->is_materialized()) {
            continue;
        }
        if (first_value) {
            first_value = false;
        } else {
            out << " ";
        }

        if (is_null(slot->null_indicator_offset())) {
            out << "null";
        } else {
            std::string value_str;
            RawValue::print_value(get_slot(slot->tuple_offset()), slot->type(), -1, &value_str);
            out << value_str;
        }
    }

    out << ")";
    return out.str();
}

std::string Tuple::to_string(const Tuple* t, const TupleDescriptor& d) {
    if (t == nullptr) {
        return "null";
    }
    return t->to_string(d);
}

} // namespace doris
