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

#include <arrow/type_fwd.h>
#include <exprs/expr.h>
#include <parquet/arrow/reader.h>
#include <parquet/encoding.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

#include <unordered_set>

#include "common/status.h"
#include "exec/arrow/parquet_reader.h"

namespace doris {

class ParquetReaderWrap;

class RowGroupReader {
public:
    RowGroupReader(int64_t range_start_offset, int64_t range_size,
                   const std::vector<ExprContext*>& conjunct_ctxs,
                   std::shared_ptr<parquet::FileMetaData>& file_metadata,
                   ParquetReaderWrap* parent);
    ~RowGroupReader();

    Status init_filter_groups(const TupleDescriptor* tuple_desc,
                              const std::map<std::string, int>& map_column,
                              const std::vector<int>& include_column_ids, int64_t file_size);

    std::unordered_set<int> filter_groups() { return _filter_group; };

private:
    void _add_filter_group(int row_group_id,
                           std::unique_ptr<parquet::RowGroupMetaData>& row_group_meta);

    int64_t _get_group_offset(int row_group_id);

    void _init_conjuncts(const TupleDescriptor* tuple_desc,
                         const std::map<std::string, int>& _map_column,
                         const std::unordered_set<int>& include_column_ids);

    bool _determine_filter_row_group(const std::vector<ExprContext*>& conjuncts,
                                     const std::string& encoded_min,
                                     const std::string& encoded_max);

    void _eval_binary_predicate(ExprContext* ctx, const char* min_bytes, const char* max_bytes,
                                bool& need_filter);

    void _eval_in_predicate(ExprContext* ctx, const char* min_bytes, const char* max_bytes,
                            bool& need_filter);

    bool _eval_in_val(PrimitiveType conjunct_type, std::vector<void*> in_pred_values,
                      const char* min_bytes, const char* max_bytes);

    bool _eval_eq(PrimitiveType conjunct_type, void* value, const char* min_bytes,
                  const char* max_bytes);

    bool _eval_gt(PrimitiveType conjunct_type, void* value, const char* max_bytes);

    bool _eval_ge(PrimitiveType conjunct_type, void* value, const char* max_bytes);

    bool _eval_lt(PrimitiveType conjunct_type, void* value, const char* min_bytes);

    bool _eval_le(PrimitiveType conjunct_type, void* value, const char* min_bytes);

private:
    int64_t _range_start_offset;
    int64_t _range_size;
    int64_t _file_size;
    std::map<int, std::vector<ExprContext*>> _slot_conjuncts;
    std::unordered_set<int> _filter_group;

    std::vector<ExprContext*> _conjunct_ctxs;
    std::shared_ptr<parquet::FileMetaData> _file_metadata;
    ParquetReaderWrap* _parent;

    int32_t _filtered_num_row_groups = 0;
    int64_t _filtered_num_rows = 0;
    int64_t _filtered_total_byte_size = 0;
};
} // namespace doris
