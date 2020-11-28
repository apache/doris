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

#ifndef DORIS_BE_SRC_OLAP_ROW_CURSOR_H
#define DORIS_BE_SRC_OLAP_ROW_CURSOR_H

#include <string>
#include <vector>

#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/row_cursor_cell.h"
#include "olap/schema.h"
#include "olap/tuple.h"

namespace doris {
class Field;

// 代理一行数据的操作
class RowCursor {
public:
    RowCursor();

    // 遍历销毁field指针
    ~RowCursor();

    // 根据传入schema的创建RowCursor
    OLAPStatus init(const TabletSchema& schema);
    OLAPStatus init(const std::vector<TabletColumn>& schema);

    // 根据传入schema的前n列创建RowCursor
    OLAPStatus init(const std::vector<TabletColumn>& schema, size_t column_count);
    OLAPStatus init(const TabletSchema& schema, size_t column_count);

    // 根据传入schema和column id list创建RowCursor，
    // 用于计算过程只使用部分非前缀连续列的场景
    OLAPStatus init(const TabletSchema& schema, const std::vector<uint32_t>& columns);

    // 用传入的key的size来初始化
    // 目前仅用在拆分key区间的时候
    OLAPStatus init_scan_key(const TabletSchema& schema, const std::vector<std::string>& keys);

    //allocate memory for string type, which include char, varchar, hyperloglog
    OLAPStatus allocate_memory_for_string_type(const TabletSchema& schema);

    RowCursorCell cell(uint32_t cid) const { return RowCursorCell(nullable_cell_ptr(cid)); }

    // RowCursor attach到一段连续的buf
    inline void attach(char* buf) { _fixed_buf = buf; }

    // 输出一列的index到buf
    void write_index_by_index(size_t index, char* index_ptr) const {
        auto dst_cell = RowCursorCell(index_ptr);
        column_schema(index)->to_index(&dst_cell, cell(index));
    }

    // deep copy field content (ignore null-byte)
    void set_field_content(size_t index, const char* buf, MemPool* mem_pool) {
        char* dest = cell_ptr(index);
        column_schema(index)->deep_copy_content(dest, buf, mem_pool);
    }

    // shallow copy field content (ignore null-byte)
    void set_field_content_shallow(size_t index, const char* buf) {
        char* dst_cell = cell_ptr(index);
        column_schema(index)->shallow_copy_content(dst_cell, buf);
    }
    // convert and deep copy field content
    OLAPStatus convert_from(size_t index, const char* src, const TypeInfo* src_type,
                            MemPool* mem_pool) {
        char* dest = cell_ptr(index);
        return column_schema(index)->convert_from(dest, src, src_type, mem_pool);
    }

    // 从传入的字符串数组反序列化内部各field的值
    // 每个字符串必须是一个\0结尾的字符串
    // 要求输入字符串和row cursor有相同的列数，
    OLAPStatus from_tuple(const OlapTuple& tuple);

    // 返回当前row cursor中列的个数
    size_t field_count() const { return _schema->column_ids().size(); }

    // 以string格式输出rowcursor内容，仅供log及debug使用
    std::string to_string() const;
    OlapTuple to_tuple() const;

    const size_t get_index_size(size_t index) const { return column_schema(index)->index_size(); }

    inline bool is_delete() const {
        auto sign_idx = _schema->delete_sign_idx();
        if (sign_idx < 0) {
            return false;
        }
        return *reinterpret_cast<const char*>(cell(sign_idx).cell_ptr()) > 0;
    }

    // set max/min for key field in _field_array
    OLAPStatus build_max_key();
    OLAPStatus build_min_key();

    inline char* get_buf() const { return _fixed_buf; }

    // this two functions is used in unit test
    inline size_t get_fixed_len() const { return _fixed_len; }
    inline size_t get_variable_len() const { return _variable_len; }

    // Get column nullable pointer with column id
    // TODO(zc): make this return const char*
    char* nullable_cell_ptr(uint32_t cid) const { return _fixed_buf + _schema->column_offset(cid); }
    char* cell_ptr(uint32_t cid) const { return _fixed_buf + _schema->column_offset(cid) + 1; }

    bool is_null(size_t index) const { return *reinterpret_cast<bool*>(nullable_cell_ptr(index)); }

    inline void set_null(size_t index) const {
        *reinterpret_cast<bool*>(nullable_cell_ptr(index)) = true;
    }

    inline void set_not_null(size_t index) const {
        *reinterpret_cast<bool*>(nullable_cell_ptr(index)) = false;
    }

    size_t column_size(uint32_t cid) const { return _schema->column_size(cid); }

    const Field* column_schema(uint32_t cid) const { return _schema->column(cid); }

    const Schema* schema() const { return _schema.get(); }

    char* row_ptr() const { return _fixed_buf; }

private:
    // common init function
    OLAPStatus _init(const std::vector<TabletColumn>& schema, const std::vector<uint32_t>& columns);

    std::unique_ptr<Schema> _schema;

    char* _fixed_buf = nullptr; // point to fixed buf
    size_t _fixed_len;
    char* _owned_fixed_buf = nullptr; // point to buf allocated in init function

    char* _variable_buf = nullptr;
    size_t _variable_len;

    DISALLOW_COPY_AND_ASSIGN(RowCursor);
};
} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROW_CURSOR_H
