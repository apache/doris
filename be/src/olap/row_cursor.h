// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BDG_PALO_BE_SRC_OLAP_ROW_CURSOR_H
#define BDG_PALO_BE_SRC_OLAP_ROW_CURSOR_H

#include <string>
#include <vector>

#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"

#define CHECK_ROWCURSOR_INIT() \
    if (!_is_inited) {\
        OLAP_LOG_FATAL("row cursor is not inited.");\
        return OLAP_ERR_NOT_INITED;\
    }

namespace palo {
class Field;

// 代理一行数据的操作
class RowCursor {
public:
    RowCursor();
    
    // 遍历销毁field指针
    ~RowCursor();
    
    // 根据传入的FieldInfo vector，创建所有的field对象
    OLAPStatus init(const std::vector<FieldInfo>& tablet_schema);

    // 根据传入schema的前n列创建RowCursor
    OLAPStatus init(const std::vector<FieldInfo>& tablet_schema, size_t column_count);

    // 根据传入schema和column id list创建RowCursor，
    // 用于计算过程只使用部分非前缀连续列的场景
    OLAPStatus init(
            const std::vector<FieldInfo>& tablet_schema, const std::vector<uint32_t>& columns);

    // 用传入的key的size来初始化
    // 目前仅用在拆分key区间的时候
    OLAPStatus init_keys(const std::vector<FieldInfo>& tablet_schema, 
                    const std::vector<std::string>& keys);
  
    // 同上，直接传keys的length
    OLAPStatus init_keys(const std::vector<FieldInfo>& tablet_schema, 
                    const std::vector<size_t>& field_lengths);
 
    // 两个RowCurosr做比较，返回-1，0，1
    int cmp(const RowCursor& other) const;

    // index_comp
    int index_cmp(const RowCursor& other) const;
    
    // 全Key比较的实现，会比默认cmp的实现要优化一些
    int full_key_cmp(const RowCursor& other) const;

    // 两个RowCurosr根据指定的fields做比较，返回true/false，比较性能好于cmp
    bool equal(const RowCursor& other) const;

    // 两个RowCursor做累加，结果是this += other
    OLAPStatus aggregate(const RowCursor& other);

    // now only used by hll column, do aggregating
    void finalize_one_merge();

    // 当前RowCursor内的数据以storage格式逐个field连续输出到一段buf中
    OLAPStatus write(char* buf) const;

    // 当前RowCursor内的数据以mysql格式逐个field连续输出到一段buf中
    OLAPStatus write_mysql(char* buf) const;

    // RowCursor从一段连续的buf中读取数据，
    OLAPStatus read(const char* buf, size_t max_buf_len);
    // read a field to RowCursor
    OLAPStatus read_field(const char* buf, size_t index, size_t field_size);

    // RowCursor attach到一段连续的buf
    inline OLAPStatus attach(char* buf, size_t max_buf_len);

    // 输出一列的值到buf
    OLAPStatus write_by_index(size_t index, char* buf) const;

    // 输出一列的index到buf
    OLAPStatus write_index_by_index(size_t index, char* buf) const;

    // 按列序号输出field的内容，传入vector，一次输出多列
    OLAPStatus write_by_indices_mysql(const std::vector<uint32_t>& indices,
                                  char* buf,
                                  size_t buf_size,
                                  size_t* written_size) const;

    // 把所有Field的格式变为MySQL格式
    inline void to_mysql();

    // 直接读取某一位置上的field的内容
    OLAPStatus read_by_index(size_t index, const char* buf);
    OLAPStatus read_by_index(size_t index, const char* buf, int length);

    // 直接attach某一位置上的field的内容
    inline OLAPStatus attach_by_index(size_t index, char* buf, bool field_by_buf);

    inline OLAPStatus set_null(size_t index);
    inline OLAPStatus set_not_null(size_t index);

    // 从传入的字符串数组反序列化内部各field的值
    // 每个字符串必须是一个\0结尾的字符串
    // 要求输入字符串和row cursor有相同的列数，
    OLAPStatus from_string(const std::vector<std::string>& val_string_arr);

    // 各field存储长度之和
    size_t length() const {
        size_t length = 0;
        for (size_t i = 0; i < _field_array_size; i++) {
            if (_field_array[i] != NULL) {
                length += _field_length_array[i];
            }
        }

        return length;
    }

    // 返回当前row cursor中列的个数
    size_t field_count() const {
        return _columns_size;
    }

    // 当前Schema是否与MySQL格式兼容, 以此判断是否需要格式转换
    bool is_mysql_compatible() const {
        return _is_mysql_compatible;
    }

    // 以string格式输出rowcursor内容，仅供log及debug使用
    std::string to_string() const;
    std::string to_string(std::string sep) const;
    std::vector<std::string> to_string_vector() const;

    // 从另外一个RowCursor复制完整的内容，需要两个cursor在字段长度和类型上完全匹配
    inline OLAPStatus copy(const RowCursor& other);
    inline OLAPStatus attach_and_copy(char* buf, const RowCursor& other);

    // 比较两个cursor，获取第一个值不同的column的id，用于selectivity的计算当中
    OLAPStatus get_first_different_column_id(const RowCursor& other, size_t* first_diff_id) const;

    const Field* get_field_by_index(size_t index) const {
        if (false == _is_inited || index >= _field_array_size) {
            return NULL;
        }

        return _field_array[index];
    }

    bool is_min(size_t index) {
        return _field_array[index]->is_min();
    }

    const size_t get_field_size(size_t index) const {
        if (false == _is_inited || index >= _field_array_size) {
            return 0;
        }
        
        return _field_array[index]->field_size();
    }

    Field* get_mutable_field_by_index(size_t index) const {
        if (false == _is_inited || index >= _field_array_size) {
            return NULL;
        }

        if (_field_array[index]->is_null()) {
            return NULL;
        }

        return _field_array[index];
    }

    // 在attach的内存位置生成最大/最小key
    OLAPStatus build_max_key();
    OLAPStatus build_min_key();

    inline OLAPStatus rearrange();

    inline const char* get_buf() const {
        return _buf;
    }   

    bool is_null(size_t i) const; //直接给定列在表中的偏移
    bool is_null_converted(size_t i) const;//给定查询中的偏移，需要转换为实际偏移

    // 用于返回row_cursor实际使用字段的长度之和,变长类型按最大长度计算
    // 比如表有10个字段，但查询仅涉及其中5个字段，则返回值为5字段长度之和
    inline size_t get_buf_len() const {
        return _length;
    }    

    // 用于清空row_cursor内部buf
    inline void reset_buf() {
        memset(_buf, 0, _length);
    }

    
    void get_field_buf_lengths(std::vector<size_t>* field_lengths) const{
        if (nullptr == field_lengths) {
            return;
        }
        
        for (int i = 0; i < _columns_size; i++) {
            field_lengths->push_back(_field_array[i]->get_buf_size());
        }
    }

private:
    // 实际的初始化函数
    OLAPStatus _init(const std::vector<FieldInfo>& tablet_schema, const std::vector<uint32_t>& columns, 
                      const std::vector<size_t>* field_lengths);

    typedef Field** field_array_t;

    enum StorageFormatEnum {
        LOCAL_STORAGE_FORMAT = 0,
        MYSQL_FORMAT = 1,
    };

    OLAPStatus _write(char* buf, StorageFormatEnum format) const;

    field_array_t _field_array;        // 内部保存field指针的数组
    size_t _null_byte_num;
    size_t _field_array_size;          // field指针数组的长度
    uint32_t* _columns;                // 部分列操作模式下的列序号数组
    size_t _columns_size;
    size_t _key_column_num;            // 一行中前多少个column是key
    size_t _length;
    size_t _length_mysql;
    size_t* _field_length_array;       // 记录每一个field的storage格式长度
    size_t* _field_offset;
    bool _is_inited;                   // 初始化标记
    char* _buf;                        // Field使用的buf
    bool _is_mysql_compatible;

    DISALLOW_COPY_AND_ASSIGN(RowCursor);
};

inline OLAPStatus RowCursor::attach(char* buf, size_t max_buf_len) {
#ifndef PERFORMANCE
    CHECK_ROWCURSOR_INIT();

    if (buf == NULL) {
        OLAP_LOG_WARNING("input pointer is NULL. [buf=%p]", buf);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

#endif
    
    size_t offset = 0;
    size_t length = 0;
    for (size_t i = 0; i < _columns_size; ++i) {
        _field_array[_columns[i]]->attach_field(buf + offset);
        length = _field_array[_columns[i]]->field_size();
        offset += length;
        _field_length_array[_columns[i]] = length;
#ifndef PERFORMANCE
        if (offset > max_buf_len) {
            OLAP_LOG_WARNING("buffer overflow. [max_buf_len=%lu offset=%lu]", max_buf_len, offset);
            return OLAP_ERR_BUFFER_OVERFLOW;
        }

#endif
    }

    return OLAP_SUCCESS;
}

void RowCursor::to_mysql() {
    for (size_t i = 0; i < _columns_size; ++i) {
        _field_array[_columns[i]]->to_mysql();
    }
}

inline OLAPStatus RowCursor::set_null(size_t index) {
    _field_array[index]->set_null();
    return OLAP_SUCCESS;
}

inline OLAPStatus RowCursor::set_not_null(size_t index) {
    _field_array[index]->set_not_null();
    return OLAP_SUCCESS;
}

inline OLAPStatus RowCursor::attach_by_index(size_t index, char* buf, bool field_or_buf) {
#ifndef PERFORMANCE
    CHECK_ROWCURSOR_INIT();

    if (index >= _field_array_size) {
        OLAP_LOG_WARNING("index exceeds the max. [index=%lu max_index=%lu]",
                         index,
                         _field_array_size);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

#endif

    if (true == field_or_buf) {
        _field_array[index]->attach_field(buf);
    } else {
        _field_array[index]->attach_buf(buf);
    }
    _field_length_array[index] = _field_array[index]->field_size();

    return OLAP_SUCCESS;
}

// 主要用于merge
inline OLAPStatus RowCursor::copy(const RowCursor& other) {
    CHECK_ROWCURSOR_INIT();
    reset_buf();

    for (size_t i = 0; i < _columns_size; ++i) {
        uint32_t column_id = _columns[i];
#ifndef PERFORMANCE
        if (column_id >= other._field_array_size || other._field_array[column_id] == NULL) {
            OLAP_LOG_WARNING("two cursor not match.");
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }

#endif
        _field_array[column_id]->copy(other._field_array[column_id]);
        // 用于merge，merge的时候其实是不需要考虑行长，因为写数据不按行长写。更新不更新都行
        _field_length_array[column_id] = other._field_array[column_id]->field_size();
    }

    return OLAP_SUCCESS;
}

// 从other拷贝到buf中。此函数仅用在读数据的时候，此处存在一个问题，
// 当varchar字段聚合时，src可能比dst长，所以在copy的时候，不能用致密的排列
// 方式。只能拷贝完之后再整理内存
inline OLAPStatus RowCursor::attach_and_copy(char* buf, const RowCursor& other) {
    CHECK_ROWCURSOR_INIT();

    size_t offset = 0;

    for (size_t i = 0; i < _columns_size; ++i) {
#ifndef PERFORMANCE
        if (_field_array[_columns[i]] == NULL
                || _columns[i] >= other._field_array_size
                || other._field_array[_columns[i]] == NULL) {
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }

#endif
        size_t field_size = other._field_array[_columns[i]]->field_size();

        // XXX(fulili) 为了实现快速copy，需要指向的buf至少有额外8字节的空间
        if (OLAP_LIKELY(field_size <= 8)) {
            memcpy(buf + offset, other._field_array[_columns[i]]->get_null(), 8);
        } else {
            memcpy(buf + offset, other._field_array[_columns[i]]->get_null(), field_size);
        }

        _field_array[_columns[i]]->attach_field(buf + offset);
        // 更新一下行长，每次attach或是from_storage之后都需要更新
        _field_length_array[_columns[i]] = field_size;
        //这里的offset 是最大值，此函数只用于查询，往buffer里顺序写行，需要算偏移
        //但由于变长字符串聚合的问题，所以不用实际长度了，聚合后调用函数紧凑下，
        //主要是防止在写入到调用函数重拍内存之间，会得到错误的长度
        //所以这里使用offset。
        offset += _field_offset[_columns[i]];
    }

    return OLAP_SUCCESS;
}

inline OLAPStatus RowCursor::rearrange() {
    size_t dst_offset = 0;
    size_t src_offset = 0;
    size_t field_length = 0;

    for (size_t i = 0; i < _columns_size; ++i) {
        field_length = _field_array[_columns[i]]->field_size();

        if (dst_offset != src_offset) {
            // dst_offset - src_offset结果为0或负数，所以此操作是将buffer前移
            memmove(_field_array[_columns[i]]->get_null() + (dst_offset - src_offset),
                    _field_array[_columns[i]]->get_null(),
                    field_length);
        }

        // offset 数组中保存的是最大偏移，且不会变
        // field_length长度只有变长字符串会不同
        src_offset += _field_offset[_columns[i]];
        dst_offset += field_length;
    }

    return OLAP_SUCCESS;
}

// RowCurosr 全key进行比较
class RowCursorComparator {
public:
    RowCursorComparator() {}
    virtual ~RowCursorComparator() {}

    bool operator()(const RowCursor* a, const RowCursor* b) const {
        return a->full_key_cmp(*b) < 0;
    }
};

}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_ROW_CURSOR_H
