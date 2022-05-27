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

#include <gen_cpp/column_data_file.pb.h>

namespace doris {

// 定义流的名字，是流的唯一标识符
// 实现比较函数，将流在文件中的顺序进行约定：
// 1. 首先排列Index流：Index流按Column unique Id排序
// 2. 再排列非Index流：首先按照column unique id，再按kind排序
class StreamName {
public:
    StreamName(uint32_t unique_column_id, StreamInfoMessage::Kind kind);

    uint32_t unique_column_id() const { return _unique_column_id; }
    StreamInfoMessage::Kind kind() const { return _kind; }

    bool operator<(const StreamName& another) const;
    bool operator==(const StreamName& another) const;

private:
    uint32_t _unique_column_id;
    StreamInfoMessage::Kind _kind;
};

} // namespace doris
