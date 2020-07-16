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

#include "olap/row.h"

namespace doris {

template <>
void copy_row_in_memtable<ContiguousRow>(ContiguousRow* dst, const ContiguousRow& src, MemPool* pool) {
    for (auto cid : dst->schema()->column_ids()) {
        auto dst_cell = dst->cell(cid);
        auto src_cell = src.cell(cid);
        dst->schema()->column(cid)->copy_object(&dst_cell, src_cell, pool);
    }
    if (dst->with_delete_flag() && src.with_delete_flag()) {
        dst->set_delete(src.is_delete());
    }
}

template <>
void agg_update_row<ContiguousRow>(ContiguousRow* dst, const ContiguousRow& src, MemPool* mem_pool) {
    for (uint32_t cid = dst->schema()->num_key_columns(); cid < dst->schema()->num_columns();
         ++cid) {
        auto dst_cell = dst->cell(cid);
        auto src_cell = src.cell(cid);
        dst->schema()->column(cid)->agg_update(&dst_cell, src_cell, mem_pool);
    }
    if (dst->with_delete_flag() && src.with_delete_flag()) {
        dst->set_delete(src.is_delete());
    }
}

template <>
void agg_update_row<ContiguousRow>(const std::vector<uint32_t>& cids, ContiguousRow* dst, const ContiguousRow& src) {
    for (auto cid : cids) {
        auto dst_cell = dst->cell(cid);
        auto src_cell = src.cell(cid);
        dst->schema()->column(cid)->agg_update(&dst_cell, src_cell);
    }
    if (dst->with_delete_flag() && src.with_delete_flag()) {
        dst->set_delete(src.is_delete());
    }
}

template <>
std::string print_row<ContiguousRow>(const ContiguousRow& row) {
    std::stringstream ss;
    
    size_t i = 0;
    for (auto cid : row.schema()->column_ids()) {
        if (i++ > 0) {
            ss << "|";
        }
        ss << row.schema()->column(cid)->debug_string(row.cell(cid));
    }
    if (row.with_delete_flag()) {
        ss << "|[ delete: " << std::boolalpha << row.is_delete() << "]";
    }

    return ss.str();
}
}  // namespace doris
