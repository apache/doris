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

#ifndef DORIS_BE_SRC_ROWSET_ROWSET_WRITER_H
#define DORIS_BE_SRC_ROWSET_ROWSET_WRITER_H

#include "rowset/rowset.h"
#include "olap/olap_define.h"
#include "olap/schema.h"
#include "olap/row_block.h"

namespace doris {

class RowsetBuilder {
public:
    // 初始化RowsetBuilder
    // rowset_id为了兼容，暂时先用string类型，后续会统一改成int64
	OLAPStatus init(std::string rowset_id, const std::string& rowset_path_prefix, Schema* schema) = 0;

    // 写入一个rowblock数据
	OLAPStatus add_row_block(RowBlock* row_block) = 0;

    // 这是一个临时接口，后续采用RowsetDeserializer/RowsetSerializer机制实现
    // 当要向一个rowset中增加一个文件的时候调用这个api，用于应对rowset改名的问题
	// 1. 根据传入的file name，rowset writer会生成一个对应的文件名
	// 2. 上游系统根据生成的文件名把文件拷贝到这个目录下
	OLAPStatus generate_written_path(const std::string& src_path, std::string* dest_path) = 0;

    // 生成一个rowset
	OLAPStatus build(Rowset* rowset) = 0;
};

}

#endif // DORIS_BE_SRC_ROWSET_ROWSET_WRITER_H