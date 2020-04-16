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

#ifndef DORIS_BE_SRC_OLAP_OLAP_COND_H
#define DORIS_BE_SRC_OLAP_OLAP_COND_H

#include <functional>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>

#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/column_data_file.pb.h"
#include "olap/bloom_filter.hpp"
#include "olap/stream_index_common.h"
#include "olap/field.h"
#include "olap/row_cursor.h"
#include "olap/rowset/segment_v2/bloom_filter.h"

namespace doris {

class WrapperField;
class RowCursorCell;

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
    size_t operator()(const WrapperField* field) const {
        return field->hash_code();
    }
};

// Equal function for IN set
struct FieldEqual {
    bool operator()(const WrapperField* left, const WrapperField* right) const {
        return left->cmp(right) == 0;
    }
};

// 条件二元组，描述了一个条件的操作类型和操作数(1个或者多个)
struct Cond {
public:
    Cond();
    ~Cond();

    OLAPStatus init(const TCondition& tcond, const TabletColumn& column);

    // 用一行数据的指定列同条件进行比较，如果符合过滤条件，
    // 即按照此条件，行应被过滤掉，则返回true，否则返回false
    bool eval(const RowCursorCell& cell) const;

    bool eval(const KeyRange& statistic) const;
    int del_eval(const KeyRange& stat) const;

    bool eval(const BloomFilter& bf) const;

    bool eval(const segment_v2::BloomFilter* bf) const;

    bool can_do_bloom_filter() const {
        return op == OP_EQ || op == OP_IN || op == OP_IS;
    }

    CondOp op;
    // valid when op is not OP_IN
    WrapperField* operand_field;
    // valid when op is OP_IN
    typedef std::unordered_set<const WrapperField*, FieldHash, FieldEqual> FieldSet;
    FieldSet operand_set;
};

// 所有归属于同一列上的条件二元组，聚合在一个CondColumn上
class CondColumn {
public:
    CondColumn(const TabletSchema& tablet_schema, int32_t index) : _col_index(index) {
        _conds.clear();
        _is_key = tablet_schema.column(_col_index).is_key();
    }
    ~CondColumn();

    // Convert condition's operand from string to Field*, and append this condition to _conds
    // return true if success, otherwise return false
    bool add_condition(Cond* condition);
    OLAPStatus add_cond(const TCondition& tcond, const TabletColumn& column);

    // 对一行数据中的指定列，用所有过滤条件进行比较，如果所有条件都满足，则过滤此行
    bool eval(const RowCursor& row) const;

    bool eval(const std::pair<WrapperField*, WrapperField*>& statistic) const;
    int del_eval(const std::pair<WrapperField*, WrapperField*>& statistic) const;

    bool eval(const BloomFilter& bf) const;

    bool eval(const segment_v2::BloomFilter* bf) const;

    bool can_do_bloom_filter() const {
        for (auto& cond : _conds) {
            if (cond->can_do_bloom_filter()) {
                // if any cond can do bloom filter
                return true;
            }
        }
        return false;
    }

    inline bool is_key() const {
        return _is_key;
    }

    const std::vector<Cond*>& conds() const {
        return _conds;
    }

private:
    bool                _is_key;
    int32_t             _col_index;
    std::vector<Cond*>   _conds;
};

// 一次请求所关联的条件
class Conditions {
public:
    // Key: field index of condition's column
    // Value: CondColumn object
    typedef std::map<int32_t, CondColumn*> CondColumns;

    Conditions() {}
    ~Conditions() {
        finalize();
    }

    void finalize() {
        for (auto& it : _columns) {
            delete it.second;
        }
        _columns.clear();
    }

    void set_tablet_schema(const TabletSchema* schema) {
        _schema = schema;
    }

    // 如果成功，则_columns中增加一项，如果失败则无视此condition，同时输出日志
    // 对于下列情况，将不会被处理
    // 1. column不属于key列
    // 2. column类型是double, float
    OLAPStatus append_condition(const TCondition& condition);
    
    bool delete_conditions_eval(const RowCursor& row) const;
    
    bool rowset_pruning_filter(const std::vector<KeyRange>& zone_maps) const;
    int delete_pruning_filter(const std::vector<KeyRange>& zone_maps) const;

    const CondColumns& columns() const {
        return _columns;
    }

    CondColumn* get_column(int32_t cid) const;

private:
    int32_t _get_field_index(const std::string& field_name) const {
        for (int i = 0; i < _schema->num_columns(); i++) {
            if (_schema->column(i).name() == field_name) {
                return i;
            }
        }
        LOG(WARNING) << "invalid field name. [name='" << field_name << "']";
        return -1;
    }

    bool _cond_column_is_key_or_duplicate(const CondColumn* cc) const {
        return cc->is_key() || _schema->keys_type() == KeysType::DUP_KEYS;
    }

private:
    const TabletSchema* _schema;
    CondColumns _columns;   // list of condition column
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_OLAP_COND_H
