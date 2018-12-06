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

#ifndef DORIS_BE_SRC_ROWSET_ROWSET_READER_H
#define DORIS_BE_SRC_ROWSET_ROWSET_READER_H

#include "olap/olap_define.h"
#include "olap/schema.h"
#include "olap/column_predicate.h"
#include "olap/row_cursor.h"
#include "olap/row_block.h"

#include <memory>

namespace doris {

struct ReadContext {
	const Schema* projection; // 投影列信息
    std::unordered_map<std::string, ColumnPredicate> predicates; //过滤条件
    const RowCursor* lower_bound_key; // key下界
    const RowCursor* exclusive_upper_bound_key; // key上界
};

class RowsetReader {
public:
    // reader初始化函数
    // init逻辑中需要判断rowset是否可以直接被统计信息过滤删除、是否被删除条件删除
    // 如果被删除，则has_next直接返回false
    OLAPStatus init(ReadContext* read_context) = 0;

    // 判断是否还有数据
    bool has_next() = 0;

	// 读取下一个Block的数据
    OLAPStatus next_block(RowBlock* row_block) = 0;

   // 关闭reader
   // 会触发下层数据读取逻辑的close操作，进行类似关闭文件，
   // 更新统计信息等操作
	void close() = 0;
};

}

#endif // DORIS_BE_SRC_ROWSET_ROWSET_READER_H