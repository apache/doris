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

#pragma once

#include <memory>

#include "olap/olap_common.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/core/types.h"
#include "vec/exprs/varray_literal.h"
#include "vec/exprs/vcast_expr.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/exprs/virtual_slot_ref.h"
#include "vec/exprs/vslot_ref.h"

namespace doris::vectorized {

class AnnTopNDescriptor {
    ENABLE_FACTORY_CREATOR(AnnTopNDescriptor);

public:
    AnnTopNDescriptor(size_t limit, VExprContextSPtr order_by_expr_ctx)
            : _limit(limit), _order_by_expr_ctx(order_by_expr_ctx) {};

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc);

    VExprContextSPtr get_order_by_expr_ctx() const { return _order_by_expr_ctx; }

    Status evaluate_vector_ann_search(segment_v2::IndexIterator* ann_index_iterator,
                                      roaring::Roaring& row_bitmap,
                                      vectorized::IColumn::MutablePtr& result_column);

    std::string debug_string() const;

    ColumnId get_src_column_id() const { return _src_column_id; }

    ColumnId get_dest_column_id() const { return _dest_column_id; }

private:
    // limit N
    const size_t _limit;
    // order by distance(xxx, [1,2])
    VExprContextSPtr _order_by_expr_ctx;

    std::string _name = "AnnTopNDescriptor";
    ColumnId _src_column_id = -1;
    ColumnId _dest_column_id = -1;
    IColumn::Ptr _query_array;
};

} // namespace doris::vectorized