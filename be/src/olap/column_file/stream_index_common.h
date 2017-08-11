// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#ifndef BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_STREAM_INDEX_COMMON_H
#define BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_STREAM_INDEX_COMMON_H

#include "olap/field.h"
#include "olap/olap_define.h"

namespace palo {
namespace column_file {

// 描述streamindex的格式
struct StreamIndexHeader {
    uint64_t block_count;           // 本index中block的个数
    uint32_t position_format;       // position的个数，每个长度为sizeof(uint32_t)
    uint32_t statistic_format;      // 统计信息格式，实际上就是OLAP_FIELD_TYPE_XXX
    // 为OLAP_FIELD_TYPE_NONE时, 表示无索引
    StreamIndexHeader() : 
            block_count(0),
            position_format(0),
            statistic_format(OLAP_FIELD_TYPE_NONE) {}
} __attribute__((packed));

// 处理列的统计信息，读写一体，也可以分开。
class ColumnStatistics {
public:
    ColumnStatistics();
    ~ColumnStatistics();

    // 初始化，需要给FieldType，用来初始化最大最小值
    // 使用前必须首先初始化，否则无效
    OLAPStatus init(const FieldType& type, bool null_supported);
    // 只是reset最大和最小值，将最小值设置为MAX，将最大值设置为MIN。
    void reset();
    // 增加一个值，根据传入值调整最大最小值
    void add(const Field* field);
    // 合并，将另一个统计信息和入当前统计中
    void merge(ColumnStatistics* other);
    // 返回最大最小值“输出时”占用的内存，而“不是�
    // ��当前结构占用的内存大小
    size_t size() const;
    // 将最大最小值attach到给定的buffer上
    void attach(char* buffer);
    // 将最大最小值输出到buffer中
    OLAPStatus write_to_buffer(char* buffer, size_t size);

    // 属性
    inline const Field* minimum() const {
        return _minimum;
    }
    inline const Field* maximum() const {
        return _maximum;
    }
    bool ignored() const {
        return _ignored;
    }
protected:
    Field* _minimum;
    Field* _maximum;
    char _buf[MAX_STATISTIC_LENGTH]; // field刚分配出来时是没有内存的，必须注意，
    // 由于暂时不支持string的统计信息，为了方便直接定义长度
    // 也可以每次都分配
    bool _ignored;
    bool _null_supported;
};

}  // namespace column_file
}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_COLUMN_FILE_STREAM_INDEX_COMMON_H

