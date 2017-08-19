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

#ifndef BDG_PALO_BE_SRC_OLAP_OLAP_COND_H
#define BDG_PALO_BE_SRC_OLAP_OLAP_COND_H

#include <functional>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>

#include "gen_cpp/column_data_file.pb.h"
#include "olap/column_file/bloom_filter.hpp"
#include "olap/column_file/stream_index_common.h"
#include "olap/field.h"
#include "olap/olap_table.h"
#include "olap/row_cursor.h"

namespace palo {
enum CondOp {
    OP_EQ = 0,      // equal
    OP_NE = 1,      // not equal
    OP_LT = 2,      // less than
    OP_LE = 3,      // less or equal
    OP_GT = 4,      // greater than
    OP_GE = 5,      // greater or equal
    OP_IN = 6,      // IN
    OP_IS = 7,      // is null or not null
    OP_NULL = 8    // invalid OP
};

// Hash functor for IN set
struct FieldHash {
    size_t operator()(const Field* field) const {
        return std::hash<std::string>()(std::string(field->buf(), field->size()));
    }
};

// Equal function for IN set
struct FieldEqual {
    bool operator()(const Field* left, const Field* right) const {
        return left->cmp(right) == 0;
    }
};

// 条件二元组，描述了一个条件的操作类型和操作数(1个或者多个)
struct Cond {
public:
    typedef std::unordered_set<const Field*, FieldHash, FieldEqual> FieldSet;

    Cond(const TCondition& condition);
    
    // Check whehter this condition is valid
    // Valid condition:
    // 1) 'op' is not null
    // 2) if 'op' is not IN, it should have only one operand
    bool validation();
    
    void finalize();
    // 用一行数据的指定列同条件进行比较，如果符合过滤条件，
    // 即按照此条件，行应被过滤掉，则返回true，否则返回false
    bool eval(const Field* field) const;
    
    bool eval(const column_file::ColumnStatistics& statistic) const;
    int del_eval(const column_file::ColumnStatistics& stat) const;

    bool eval(const std::pair<Field *, Field *>& statistic) const;
    int del_eval(const std::pair<Field *, Field *>& stat) const;

    bool eval(const column_file::BloomFilter& bf) const;
    
    // 封装Field::create以及分配attach使用的buffer
    Field* create_field(const FieldInfo& fi);

    Field* create_field(const FieldInfo& fi, uint32_t len);

    CondOp                      op;
    std::string                 column_name;
    std::string                 condition_string;
    std::vector<std::string>    operands;         // 所有操作数的字符表示
    Field*                      operand_field;    // 如果不是OP_IN, 此处保存唯一操作数
    FieldSet                    operand_set;      // 如果是OP_IN，此处为IN的集合

private:
    std::vector<char*>         operand_field_buf;  // buff for field.attach
};

// 所有归属于同一列上的条件二元组，聚合在一个CondColumn上
class CondColumn {
public:
    CondColumn() : _is_key(true), _col_index(0) {}
    
    CondColumn(SmartOLAPTable table, int32_t index) : _col_index(index), _table(table) {
        _conds.clear();
        _is_key = _table->tablet_schema()[_col_index].is_key;
    }

    CondColumn(const CondColumn& from);

    // Convert condition's operand from string to Field*, and append this condition to _conds
    // return true if success, otherwise return false
    bool add_condition(Cond* condition);

    // 对一行数据中的指定列，用所有过滤条件进行比较，如果所有条件都满足，则过滤此行
    bool eval(const RowCursor& row) const;
    
    bool eval(const column_file::ColumnStatistics& statistic) const;
    int del_eval(const column_file::ColumnStatistics& col_stat) const;

    bool eval(const std::pair<Field *, Field *>& statistic) const;
    int del_eval(const std::pair<Field *, Field *>& statistic) const;

    bool eval(const column_file::BloomFilter& bf) const;

    void finalize();

    inline bool is_key() const {
        return _is_key;
    }

    const std::vector<Cond>& conds() const {
        return _conds;
    }

private:
    bool                _is_key;
    int32_t             _col_index;
    std::vector<Cond>   _conds;
    SmartOLAPTable      _table;
};

// 一次请求所关联的条件
class Conditions {
public:
    // Key: field index of condition's column
    // Value: CondColumn object
    typedef std::map<int32_t, CondColumn> CondColumns;

    Conditions() {}

    Conditions& operator=(const Conditions& conds) {
        if (&conds != this) {
            _columns = conds._columns;
            _table = conds._table;
        }

        return *this;
    }

    void finalize() {
        for (CondColumns::iterator it = _columns.begin(); it != _columns.end(); ++it) {
            it->second.finalize();
        }
        _columns.clear();
    }

    void set_table(SmartOLAPTable table) {
        long do_not_remove_me_until_you_want_a_heart_attacking = table.use_count();
        OLAP_UNUSED_ARG(do_not_remove_me_until_you_want_a_heart_attacking);

        _table = table;
    }

    // 如果成功，则_columns中增加一项，如果失败则无视此condition，同时输出日志
    // 对于下列情况，将不会被处理
    // 1. column不属于key列
    // 2. column类型是double, float
    OLAPStatus append_condition(const TCondition& condition);
    
    bool delete_conditions_eval(const RowCursor& row) const;

    int delete_conditions_eval(const column_file::ColumnStatistics& col_stat) const;
    
    bool where_conditions_eval(uint32_t field_index,
                               const column_file::ColumnStatistics& statistic) const;

    bool delta_pruning_filter(std::vector<std::pair<Field *, Field *>> &column_statistics) const;
    int delete_pruning_filter(std::vector<std::pair<Field *, Field *>> &column_statistics) const;


    const CondColumns& columns() const {
        return _columns;
    }

private:
    SmartOLAPTable _table;     // ref to OLAPTable to access schema
    CondColumns _columns;   // list of condition column
};

}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_OLAP_COND_H
