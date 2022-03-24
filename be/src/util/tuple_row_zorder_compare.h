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

#ifndef DORIS_TUPLE_ROW_ZORDER_COMPARE_H
#define DORIS_TUPLE_ROW_ZORDER_COMPARE_H

#include "exec/sort_exec_exprs.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "olap/row.h"
#include "olap/row_cursor.h"
#include "olap/schema.h"
#include "runtime/descriptors.h"
#include "runtime/raw_value.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"

namespace doris {
class RowComparator {
public:
    RowComparator() = default;
    RowComparator(Schema* schema);
    virtual ~RowComparator() = default;
    virtual int operator()(const char* left, const char* right) const;
};

class TupleRowZOrderComparator : public RowComparator {
private:
    typedef __uint128_t uint128_t;
    int _max_col_size = 0;
    const Schema* _schema;
    int _sort_col_num = 0;

public:
    TupleRowZOrderComparator();
    TupleRowZOrderComparator(int sort_col_num);
    TupleRowZOrderComparator(Schema* schema, int sort_col_num);
    virtual ~TupleRowZOrderComparator() {}
    int compare(const char* lhs, const char* rhs) const;
    void max_col_size(const RowCursor& rc);
    int compare_row(const RowCursor& lhs, const RowCursor& rhs);
    template <typename U, typename LhsRowType>
    int compare_based_on_size(LhsRowType& lhs, LhsRowType& rhs) const;
    template <typename U>
    U get_shared_representation(const void* val, FieldType type) const;
    template <typename U, typename T>
    U inline get_shared_int_representation(const T val, U mask) const;
    template <typename U, typename T>
    U inline get_shared_float_representation(const void* val, U mask) const;
    template <typename U>
    U inline get_shared_string_representation(const char* char_ptr, int length) const;
    virtual int operator()(const char* lhs, const char* rhs) const;
    int get_type_byte_size(FieldType type) const;
};
} // namespace doris

#endif //DORIS_TUPLE_ROW_ZORDER_COMPARE_H