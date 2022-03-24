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

#ifndef DORIS_BE_RUNTIME_TUPLE_H
#define DORIS_BE_RUNTIME_TUPLE_H

#include <cstring>

#include "common/logging.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"

namespace doris {

struct StringValue;
struct CollectionValue;
class TupleDescriptor;
class DateTimeValue;
class TupleRow;
class ExprContext;

// A tuple is stored as a contiguous sequence of bytes containing a fixed number
// of fixed-size slots. The slots are arranged in order of increasing byte length;
// the tuple might contain padding between slots in order to align them according
// to their type.
//
// The contents of a tuple:
// 1) a number of bytes holding a bitvector of null indicators
// 2) bool slots
// 3) tinyint slots
// 4) smallint slots
// 5) int slots
// 6) float slots
// 7) bigint slots
// 8) double slots
// 9) string slots
class Tuple {
public:
    // initialize individual tuple with data residing in mem pool
    static Tuple* create(int size, MemPool* pool) {
        // assert(size > 0);
        Tuple* result = reinterpret_cast<Tuple*>(pool->allocate(size));
        result->init(size);
        return result;
    }

    void init(int size) { bzero(_data, size); }

    // The total size of all data represented in this tuple (tuple data and referenced
    // string and collection data).
    int64_t total_byte_size(const TupleDescriptor& desc) const;

    // The size of all referenced string and collection data.
    int64_t varlen_byte_size(const TupleDescriptor& desc) const;

    // create a copy of 'this', including all of its referenced string data,
    // using pool to allocate memory. Returns the copy.
    // If 'convert_ptrs' is true, converts pointers that are part of the tuple
    // into offsets in 'pool'.
    Tuple* deep_copy(const TupleDescriptor& desc, MemPool* pool, bool convert_ptrs);

    Tuple* deep_copy(const TupleDescriptor& desc, MemPool* pool) {
        return deep_copy(desc, pool, false);
    }

    // create a copy of 'this', including all its referenced string data.  This
    // version does not allocate a tuple, instead copying 'dst'.  dst must already
    // be allocated to the correct size (desc.byte_size())
    // If 'convert_ptrs' is true, converts pointers that are part of the tuple
    // into offsets in 'pool'.
    void deep_copy(Tuple* dst, const TupleDescriptor& desc, MemPool* pool, bool convert_ptrs);
    void deep_copy(Tuple* dst, const TupleDescriptor& desc, MemPool* pool) {
        deep_copy(dst, desc, pool, false);
    }

    // deep copy use 'new', must be 'free' after use
    Tuple* dcopy_with_new(const TupleDescriptor& desc, MemPool* pool, int64_t* bytes);
    int64_t dcopy_with_new(Tuple* dst, const TupleDescriptor& desc);
    int64_t release_string(const TupleDescriptor& desc);

    // create a copy of 'this', including all referenced string data, into
    // data. The tuple is written first, followed by any strings. data and offset
    // will be incremented by the total number of bytes written. data must already
    // be allocated to the correct size.
    // If 'convert_ptrs' is true, converts pointers that are part of the tuple
    // into offsets in data, based on the provided offset. Otherwise they will be
    // pointers directly into data.
    void deep_copy(const TupleDescriptor& desc, char** data, int64_t* offset, bool convert_ptrs);
    void deep_copy(const TupleDescriptor& desc, char** data, int64_t* offset) {
        deep_copy(desc, data, offset, false);
    }

    // Materialize this by evaluating the expressions in materialize_exprs
    // over the specified 'row'. 'pool' is used to allocate var-length data.
    // (Memory for this tuple itself must already be allocated.)
    // If collect_string_vals is true, the materialized non-nullptr string value
    // slots and the total length of the string slots are returned in var_values
    // and total_var_len.
    template <bool collect_string_vals>
    void materialize_exprs(TupleRow* row, const TupleDescriptor& desc,
                           const std::vector<ExprContext*>& materialize_expr_ctxs, MemPool* pool,
                           std::vector<StringValue*>* non_null_var_len_values, int* total_var_len);

    // Turn null indicator bit on.
    // Turn null indicator bit on. For non-nullable slots, the mask will be 0 and
    // this is a no-op (but we don't have to branch to check is slots are nulalble).
    void set_null(const NullIndicatorOffset& offset) {
        //DCHECK(offset.bit_mask != 0);
        char* null_indicator_byte = &_data[offset.byte_offset];
        *null_indicator_byte |= offset.bit_mask;
    }

    // Turn null indicator bit off.
    void set_not_null(const NullIndicatorOffset& offset) {
        char* null_indicator_byte = &_data[offset.byte_offset];
        *null_indicator_byte &= ~offset.bit_mask;
    }

    bool is_null(const NullIndicatorOffset& offset) const {
        const char* null_indicator_byte = &_data[offset.byte_offset];
        return (*null_indicator_byte & offset.bit_mask) != 0;
    }

    void* get_slot(int offset) {
        DCHECK(offset != -1); // -1 offset indicates non-materialized slot
        return &_data[offset];
    }

    const void* get_slot(int offset) const {
        DCHECK(offset != -1); // -1 offset indicates non-materialized slot
        return &_data[offset];
    }

    StringValue* get_string_slot(int offset) {
        DCHECK(offset != -1); // -1 offset indicates non-materialized slot
        return reinterpret_cast<StringValue*>(&_data[offset]);
    }

    const StringValue* get_string_slot(int offset) const {
        DCHECK(offset != -1); // -1 offset indicates non-materialized slot
        return reinterpret_cast<const StringValue*>(&_data[offset]);
    }

    CollectionValue* get_collection_slot(int offset) {
        DCHECK(offset != -1); // -1 offset indicates non-materialized slot
        return reinterpret_cast<CollectionValue*>(&_data[offset]);
    }

    const CollectionValue* get_collection_slot(int offset) const {
        DCHECK(offset != -1); // -1 offset indicates non-materialized slot
        return reinterpret_cast<const CollectionValue*>(&_data[offset]);
    }

    DateTimeValue* get_datetime_slot(int offset) {
        DCHECK(offset != -1); // -1 offset indicates non-materialized slot
        return reinterpret_cast<DateTimeValue*>(&_data[offset]);
    }

    DecimalV2Value* get_decimalv2_slot(int offset) {
        DCHECK(offset != -1); // -1 offset indicates non-materialized slot
        return reinterpret_cast<DecimalV2Value*>(&_data[offset]);
    }

    void* get_data() { return _data; }

    std::string to_string(const TupleDescriptor& d) const;
    static std::string to_string(const Tuple* t, const TupleDescriptor& d);

private:
    char _data[0];
};

} // namespace doris

#endif
