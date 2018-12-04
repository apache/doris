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
#include "olap/tuple.h"

namespace doris {
class Field;

// 代理一行数据的操作
class RowCursor {
public:
    static inline bool equal(const std::vector<uint32_t>& ids,
                             const RowCursor* lhs, const RowCursor* rhs) {
        for (auto id : ids) {
            char* left = lhs->get_field_ptr(id);
            char* right = rhs->get_field_ptr(id);
            if (!lhs->_field_array[id]->equal(left, right)) {
                return false;
            }
        }
        return true;
    }

    static inline void aggregate(const std::vector<uint32_t>& cids,
                                 RowCursor* lhs, const RowCursor* rhs) {
        // 只有value column才会参与aggregate
        for (auto cid : cids) {
            char* dest = lhs->get_field_ptr(cid);
            char* src = rhs->get_field_ptr(cid);
            lhs->_field_array[cid]->aggregate(dest, src);
        }
    }

    RowCursor();
    
    // 遍历销毁field指针
    ~RowCursor();
    
    // 根据传入schema的创建RowCursor
    OLAPStatus init(const TabletSchema& schema);
    OLAPStatus init(const std::vector<TabletColumn>& schema);

    // 根据传入schema的前n列创建RowCursor
    OLAPStatus init(const std::vector<TabletColumn>& schema,
                    size_t column_count);
    OLAPStatus init(const TabletSchema& schema, size_t column_count);

    // 根据传入schema和column id list创建RowCursor，
    // 用于计算过程只使用部分非前缀连续列的场景
    OLAPStatus init(const TabletSchema& schema,
                    const std::vector<uint32_t>& columns);

    // 用传入的key的size来初始化
    // 目前仅用在拆分key区间的时候
    OLAPStatus init_scan_key(const TabletSchema& schema,
                             const std::vector<std::string>& keys);

    //allocate memory for string type, which include char, varchar, hyperloglog
    OLAPStatus allocate_memory_for_string_type(const TabletSchema& schema,
                                               MemPool* mem_pool = nullptr);

    // 两个RowCurosr做比较，返回-1，0，1
    int cmp(const RowCursor& other) const;

    // index_comp
    int index_cmp(const RowCursor& other) const;
    
    // 全Key比较的实现，会比默认cmp的实现要优化一些
    int full_key_cmp(const RowCursor& other) const;

    // 两个RowCurosr根据指定的fields做比较，返回true/false，比较性能好于cmp
    bool equal(const RowCursor& other) const;

    // 两个RowCursor做累加，结果是this += other
    void aggregate(const RowCursor& other);

    // now only used by hll column, do aggregating
    void finalize_one_merge();
    inline void finalize_one_merge(const std::vector<uint32_t>& ids);

    // RowCursor attach到一段连续的buf
    inline void attach(char* buf) { _fixed_buf = buf; }

    // 输出一列的index到buf
    void write_index_by_index(size_t index, char* index_ptr) const {
        char* src = _field_array[index]->get_field_ptr(_fixed_buf);
        _field_array[index]->to_index(index_ptr, src);
    }

    // set field content without nullbyte
    void set_field_content(size_t index, const char* buf, MemPool* mem_pool) {
        char* dest = _field_array[index]->get_ptr(_fixed_buf);
        _field_array[index]->copy_content(dest, buf, mem_pool);
    }

    inline void set_null(size_t index) { _field_array[index]->set_null(_fixed_buf); }
    inline void set_not_null(size_t index) { _field_array[index]->set_not_null(_fixed_buf); } 

    // 从传入的字符串数组反序列化内部各field的值
    // 每个字符串必须是一个\0结尾的字符串
    // 要求输入字符串和row cursor有相同的列数，
    OLAPStatus from_tuple(const OlapTuple& tuple);

    // 返回当前row cursor中列的个数
    size_t field_count() const {
        return _columns.size();
    }

    // 以string格式输出rowcursor内容，仅供log及debug使用
    std::string to_string() const;
    std::string to_string(std::string sep) const;
    OlapTuple to_tuple() const;

    // 从另外一个RowCursor复制完整的内容，需要两个cursor在字段长度和类型上完全匹配
    inline OLAPStatus copy(const RowCursor& other, MemPool* mem_pool);
    inline OLAPStatus copy_without_pool(const RowCursor& other);
    inline OLAPStatus agg_init(const RowCursor& other);

    // 比较两个cursor，获取第一个值不同的column的id，用于selectivity的计算当中
    OLAPStatus get_first_different_column_id(const RowCursor& other, size_t* first_diff_id) const;

    const Field* get_field_by_index(size_t index) const {
        return _field_array[index];
    }

    bool is_min(size_t index) {
        Field* field = _field_array[index];
        char* src = field->get_ptr(_fixed_buf);
        return field->is_min(src);
    }

    const size_t get_index_size(size_t index) const {
        return _field_array[index]->index_size();
    }

    // set max/min for key field in _field_array
    OLAPStatus build_max_key();
    OLAPStatus build_min_key();

    inline char* get_buf() const { return _fixed_buf; }

    // this two functions is used in unit test
    inline size_t get_fixed_len() const { return _fixed_len; }
    inline size_t get_variable_len() const { return _variable_len; }

    bool is_null(size_t index) const {
        return _field_array[index]->is_null(_fixed_buf);
    }

    char* get_field_ptr(uint32_t cid) const { return _fixed_buf + _field_offsets[cid]; }
    char* get_field_content_ptr(uint32_t cid) const { return _fixed_buf + _field_offsets[cid] + 1; }
    size_t get_field_offset(uint32_t cid) const { return _field_offsets[cid] + 1; }
    std::vector<uint32_t>& get_string_columns() { return _string_columns; }

    inline uint32_t hash_code(uint32_t seed) const;
private:
    // common init function
    OLAPStatus _init(const std::vector<TabletColumn>& schema,
                     const std::vector<uint32_t>& columns);

    std::vector<Field*> _field_array;    // store point array of field
    std::vector<size_t> _field_offsets;  // field offset in _fixed_buf

    size_t _key_column_num;              // key num in row_cursor

    std::vector<uint32_t> _columns;      // column_id in schema
    std::vector<uint32_t> _string_columns;      // column_id in schema
    char* _fixed_buf = nullptr;          // point to fixed buf
    size_t _fixed_len;
    char* _owned_fixed_buf = nullptr;    // point to buf allocated in init function

    char* _variable_buf = nullptr;
    size_t _variable_len;
    bool _variable_buf_allocated_by_pool;
    std::vector<HllContext*> hll_contexts;

    DISALLOW_COPY_AND_ASSIGN(RowCursor);
};

// 主要用于merge
inline OLAPStatus RowCursor::copy(const RowCursor& other, MemPool* mem_pool) {
    for (auto cid : _columns) {
        Field* field = _field_array[cid];
        char* dest = get_field_ptr(cid);
        char* src = other.get_field_ptr(cid);
        field->copy_with_pool(dest, src, mem_pool);
    }

    return OLAP_SUCCESS;
}

inline OLAPStatus RowCursor::copy_without_pool(const RowCursor& other) {
    for (auto cid : _columns) {
        Field* field = _field_array[cid];
        char* dest = get_field_ptr(cid);
        char* src = other.get_field_ptr(cid);
        field->copy_without_pool(dest, src);
    }

    return OLAP_SUCCESS;
}

inline OLAPStatus RowCursor::agg_init(const RowCursor& other) {
    for (auto cid : _columns) {
        Field* field = _field_array[cid];
        char* dest = get_field_ptr(cid);
        char* src = other.get_field_ptr(cid);
        field->agg_init(dest, src);
    }

    return OLAP_SUCCESS;
}

inline void RowCursor::finalize_one_merge(const std::vector<uint32_t>& ids) {
    for (uint32_t id : ids) {
        char* dest = _field_array[id]->get_ptr(_fixed_buf);
        _field_array[id]->finalize(dest);
    }
}

inline uint32_t RowCursor::hash_code(uint32_t seed) const {
    for (auto cid : _columns) {
        char* dest = _field_array[cid]->get_field_ptr(_fixed_buf);
        seed = _field_array[cid]->hash_code(dest, seed);
    }
    return seed;
}

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROW_CURSOR_H
