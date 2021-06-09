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

#ifndef DORIS_BE_SRC_OLAP_COLUMN_FILE_STREAM_INDEX_COMMON_H
#define DORIS_BE_SRC_OLAP_COLUMN_FILE_STREAM_INDEX_COMMON_H

#include <functional>

#include "olap/field.h"
#include "olap/olap_define.h"
#include "olap/wrapper_field.h"

namespace doris {

// 描述streamindex的格式
struct StreamIndexHeader {
    uint64_t block_count;      // 本index中block的个数
    uint32_t position_format;  // position的个数，每个长度为sizeof(uint32_t)
    uint32_t statistic_format; // 统计信息格式，实际上就是OLAP_FIELD_TYPE_XXX
    // 为OLAP_FIELD_TYPE_NONE时, 表示无索引
    StreamIndexHeader()
            : block_count(0), position_format(0), statistic_format(OLAP_FIELD_TYPE_NONE) {}
} __attribute__((packed));

// TODO: string type(char, varchar) has no columnar statistics at present.
// when you want to add columnar statistics for string type,
// don't forget to convert storage layout between disk and memory.
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

    template <typename CellType>
    inline void add(const CellType& cell) {
        if (_ignored) {
            return;
        }
        if (_maximum->field()->compare_cell(*_maximum, cell) < 0) {
            _maximum->field()->direct_copy(_maximum, cell);
        }
        if (_minimum->field()->compare_cell(*_minimum, cell) > 0) {
            _minimum->field()->direct_copy(_minimum, cell);
        }
    }

    // 合并，将另一个统计信息和入当前统计中
    void merge(ColumnStatistics* other);
    // 返回最大最小值“输出时”占用的内存，而“不是?
    // ??当前结构占用的内存大小
    size_t size() const;
    // 将最大最小值attach到给定的buffer上
    void attach(char* buffer);
    // 将最大最小值输出到buffer中
    OLAPStatus write_to_buffer(char* buffer, size_t size);

    // 属性
    const WrapperField* minimum() const { return _minimum; }
    const WrapperField* maximum() const { return _maximum; }
    std::pair<WrapperField*, WrapperField*> pair() const {
        return std::make_pair(_minimum, _maximum);
    }
    bool ignored() const { return _ignored; }

protected:
    WrapperField* _minimum;
    WrapperField* _maximum;
    // 由于暂时不支持string的统计信息，为了方便直接定义长度
    // 也可以每次都分配
    bool _ignored;
    bool _null_supported;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_COLUMN_FILE_STREAM_INDEX_COMMON_H
