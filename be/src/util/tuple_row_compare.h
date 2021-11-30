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

#ifndef INF_DORIS_BE_SRC_UTIL_TUPLE_ROW_COMPARE_H
#define INF_DORIS_BE_SRC_UTIL_TUPLE_ROW_COMPARE_H

#include "exec/sort_exec_exprs.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "runtime/descriptors.h"
#include "runtime/raw_value.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"

namespace doris {

class TupleRowComparator {
public:
    // Compares two TupleRows based on a set of exprs, in order.
    // We use is_asc to determine, for each expr, if it should be ascending or descending
    // sort order.
    // We use nulls_first to determine, for each expr, if nulls should come before
    // or after all other values.
    TupleRowComparator(const std::vector<ExprContext*>& key_expr_ctxs_lhs,
                       const std::vector<ExprContext*>& key_expr_ctxs_rhs,
                       const std::vector<bool>& is_asc, const std::vector<bool>& nulls_first)
            : _key_expr_ctxs_lhs(key_expr_ctxs_lhs),
              _key_expr_ctxs_rhs(key_expr_ctxs_rhs),
              _is_asc(is_asc) {
        // DCHECK_EQ(key_expr_ctxs_lhs.size(), key_expr_ctxs_rhs.size());
        DCHECK_EQ(key_expr_ctxs_lhs.size(), is_asc.size());
        DCHECK_EQ(key_expr_ctxs_lhs.size(), nulls_first.size());
        _nulls_first.reserve(key_expr_ctxs_lhs.size());
        for (int i = 0; i < key_expr_ctxs_lhs.size(); ++i) {
            _nulls_first.push_back(nulls_first[i] ? -1 : 1);
        }
    }

    TupleRowComparator(const std::vector<ExprContext*>& key_expr_ctxs_lhs,
                       const std::vector<ExprContext*>& key_expr_ctxs_rhs, bool is_asc,
                       bool nulls_first)
            : _key_expr_ctxs_lhs(key_expr_ctxs_lhs),
              _key_expr_ctxs_rhs(key_expr_ctxs_rhs),
              _is_asc(key_expr_ctxs_lhs.size(), is_asc),
              _nulls_first(key_expr_ctxs_lhs.size(), nulls_first ? -1 : 1) {
        DCHECK_EQ(key_expr_ctxs_lhs.size(), key_expr_ctxs_rhs.size());
    }

    // 'sort_key_exprs' must have already been prepared.
    // 'is_asc' determines, for each expr, if it should be ascending or descending sort
    // order.
    // 'nulls_first' determines, for each expr, if nulls should come before or after all
    // other values.
    TupleRowComparator(const SortExecExprs& sort_key_exprs, const std::vector<bool>& is_asc,
                       const std::vector<bool>& nulls_first)
            : _key_expr_ctxs_lhs(sort_key_exprs.lhs_ordering_expr_ctxs()),
              _key_expr_ctxs_rhs(sort_key_exprs.rhs_ordering_expr_ctxs()),
              _is_asc(is_asc) {
        DCHECK_EQ(_key_expr_ctxs_lhs.size(), is_asc.size());
        DCHECK_EQ(_key_expr_ctxs_lhs.size(), nulls_first.size());
        _nulls_first.reserve(_key_expr_ctxs_lhs.size());
        for (int i = 0; i < _key_expr_ctxs_lhs.size(); ++i) {
            _nulls_first.push_back(nulls_first[i] ? -1 : 1);
        }
    }

    TupleRowComparator(const SortExecExprs& sort_key_exprs, bool is_asc, bool nulls_first)
            : _key_expr_ctxs_lhs(sort_key_exprs.lhs_ordering_expr_ctxs()),
              _key_expr_ctxs_rhs(sort_key_exprs.rhs_ordering_expr_ctxs()),
              _is_asc(_key_expr_ctxs_lhs.size(), is_asc),
              _nulls_first(_key_expr_ctxs_lhs.size(), nulls_first ? -1 : 1) {}

    // Returns a negative value if lhs is less than rhs, a positive value if lhs is greater
    // than rhs, or 0 if they are equal. All exprs (_key_exprs_lhs and _key_exprs_rhs)
    // must have been prepared and opened before calling this. i.e. 'sort_key_exprs' in the
    // constructor must have been opened.
    int compare(TupleRow* lhs, TupleRow* rhs) const {
        for (int i = 0; i < _key_expr_ctxs_lhs.size(); ++i) {
            void* lhs_value = _key_expr_ctxs_lhs[i]->get_value(lhs);
            void* rhs_value = _key_expr_ctxs_rhs[i]->get_value(rhs);

            // The sort order of NULLs is independent of asc/desc.
            if (lhs_value == nullptr && rhs_value == nullptr) {
                continue;
            }
            if (lhs_value == nullptr && rhs_value != nullptr) {
                return _nulls_first[i];
            }
            if (lhs_value != nullptr && rhs_value == nullptr) {
                return -_nulls_first[i];
            }

            int result =
                    RawValue::compare(lhs_value, rhs_value, _key_expr_ctxs_lhs[i]->root()->type());
            if (!_is_asc[i]) {
                result = -result;
            }
            if (result != 0) {
                return result;
            }
            // Otherwise, try the next Expr
        }
        return 0; // fully equivalent key
    }

    // Returns true if lhs is strictly less than rhs.
    // All exprs (_key_exprs_lhs and _key_exprs_rhs) must have been prepared and opened
    // before calling this.
    bool operator()(TupleRow* lhs, TupleRow* rhs) const {
        int result = compare(lhs, rhs);
        if (result < 0) {
            return true;
        }
        return false;
    }

    bool operator()(Tuple* lhs, Tuple* rhs) const {
        TupleRow* lhs_row = reinterpret_cast<TupleRow*>(&lhs);
        TupleRow* rhs_row = reinterpret_cast<TupleRow*>(&rhs);
        return (*this)(lhs_row, rhs_row);
    }

private:
    const std::vector<ExprContext*>& _key_expr_ctxs_lhs;
    const std::vector<ExprContext*>& _key_expr_ctxs_rhs;
    std::vector<bool> _is_asc;
    std::vector<int8_t> _nulls_first;

    typedef int (*CompareFn)(ExprContext* const*, ExprContext* const*, TupleRow*, TupleRow*);
};
} // namespace doris

#endif
