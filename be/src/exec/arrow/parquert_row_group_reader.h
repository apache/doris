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

#include "common/status.h"

#include <unordered_set>
#include <exprs/expr.h>
#include <arrow/type_fwd.h>
#include <parquet/file_reader.h>
#include <parquet/arrow/reader.h>
#include <parquet/metadata.h>
#include <parquet/statistics.h>
#include <parquet/types.h>
#include <parquet/encoding.h>

namespace doris {

    class ParquetReaderWrap;

    struct ParquetPredicate {
//    SlotRef
//    op
//
    };


// binary predicate


    class RowGroupReader {
    public:
        RowGroupReader(const std::vector<ExprContext*>& conjunct_ctxs,
                       std::shared_ptr<parquet::FileMetaData>& file_metadata);
        ~RowGroupReader();

        Status init_filter_groups(const std::vector<SlotDescriptor*>& tuple_slot_descs,
                                  const std::map<std::string, int>& map_column,
                                  const std::vector<int>& include_column_ids);

        bool has_filter_groups() { return _filter_group.empty(); };

        std::unordered_set<int> filter_groups() { return _filter_group; };

    private:
        void _init_conjuncts(const std::vector<SlotDescriptor*>& tuple_slot_descs,
                             const std::map<std::string, int>& _map_column,
                             const std::unordered_set<int>& include_column_ids);

        Status _determine_filter_row_group(int row_group, const std::vector<Expr*>& conjuncts,
                                           const std::string& min, const std::string& max, bool* need_filter);

        Status _eval_binary_predicate(const TExprOpcode::type op_type, const Expr* conjunct,
                                      const std::string& min, const std::string& max, bool* need_filter);

        Status _eval_in_predicate(const TExprOpcode::type op_type, const Expr* conjunct,
                                  const std::string& min, const std::string& max, bool* need_filter);

    private:
        std::map<int, std::vector<Expr*>> _slot_conjuncts;
        std::unordered_set<int> _filter_group;

        std::vector<ExprContext*> _conjunct_ctxs;
        std::shared_ptr<parquet::FileMetaData> _file_metadata;
    };
}


