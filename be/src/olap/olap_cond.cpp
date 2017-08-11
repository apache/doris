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

#include "olap/olap_cond.h"

#include <cstring>
#include <string>
#include <utility>
#include <thrift/protocol/TDebugProtocol.h>

#include "olap/olap_define.h"
#include "olap/utils.h"

using std::nothrow;
using std::pair;
using std::string;
using std::vector;

using palo::column_file::ColumnStatistics;

//此文件主要用于对用户发送的查询条件和删除条件进行处理，逻辑上二者都可以分为三层
//Condtiion->Condcolumn->Cond
//Condition表示用户发的单个条件
//Condcolumn表示一列上所有条件的集合。
//Conds表示一列上的单个条件.
//对于查询条件而言，各层级的条件之间都是逻辑与的关系
//对于delete条件则有不同。Cond和Condcolumn之间是逻辑与的关系，而Condtion直接是逻辑或的关系。

//具体到实现。
//eval是用来过滤查询条件，包括堆row、block、version的过滤，具体使用哪一层看具体的调用地方。
//  1. 没有单独过滤行的过滤条件，这部分在查询层进行。
//  2. 过滤block在SegmentReader里面。
//  3. 过滤version在Reader里面。调用delta_pruing_filter
//
//del_eval用来过滤删除条件，包括堆block和version的过滤，但是这个过滤比eval多了一个状态，即部分过滤。
//  1. 对行的过滤在DeleteHandler。
//     这部分直接调用delete_condition_eval实现,内部调用eval函数，因为对row的过滤不涉及部分过滤这种状态。
//  2. 过滤block是在SegmentReader里面,直接调用del_eval
//  3. 过滤version实在Reader里面,调用delta_pruning_filter

namespace palo {

static CondOp parse_op_type(const string& op) {
    if (op.size() > 2) {
        return OP_NULL;
    }

    CondOp op_type = OP_NULL;
    if (op.compare("=") == 0) {
        return OP_EQ;
    }

    if (0 == strcasecmp(op.c_str(), "is")) {
        return OP_IS;
    }

    // Maybe we can just use string compare.
    // Like:
    //     if (op == "!=") {
    //         op_type = OP_NE;
    //     } else if (op == "*") {
    //         op_type = OP_IN;
    //     } else if (op == ">=) {
    //     ...    

    switch (op.c_str()[0]) {
    case '!':
        op_type = OP_NE;
        break;
    case '*':
        op_type = OP_IN;
        break;
    case '>':
        switch (op.c_str()[1]) {
        case '=':
            op_type = OP_GE;
            break;
        default:
            op_type = OP_GT;
            break;
        }
        break;
    case '<':
        switch (op.c_str()[1]) {
        case '=':
            op_type = OP_LE;
            break;
        default:
            op_type = OP_LT;
            break;
        }
        break;
    default:
        op_type = OP_NULL;
        break;
    }

    return op_type;
}

Cond::Cond(const TCondition& condition)
        : condition_string(apache::thrift::ThriftDebugString(condition)) {
    OLAP_LOG_DEBUG("parsing expr. [cond_expr=%s]", condition_string.c_str());
    
    column_name = condition.column_name;
    op = parse_op_type(condition.condition_op);
    operands = condition.condition_values;

    operand_field = NULL;
}

bool Cond::validation() {
    if (op == OP_NULL || (op != OP_IN && operands.size() != 1)) {
        return false;
    }

    return true;
}

void Cond::finalize() {
    if (op == OP_IN) {
        for (FieldSet::const_iterator it = operand_set.begin(); it != operand_set.end();) {
            const Field *tmp = *it;
            operand_set.erase(it++);
            SAFE_DELETE(tmp);
        }
    } else {
        SAFE_DELETE(operand_field);
    }

    for (vector<char*>::const_iterator it = operand_field_buf.begin();
            it != operand_field_buf.end(); ++it) {
        delete [] *it;
    }
    operand_field_buf.clear();

    operands.clear();
}

bool Cond::eval(const Field* field) const {
    //通过单列上的单个查询条件对row进行过滤
    if (field == NULL) {
        OLAP_LOG_WARNING("null operand for evaluation. [condition=%s]", condition_string.c_str());
        return false;
    }
    if (field->is_null() && op != OP_IS) {
        //任何operand和NULL的运算都是false
        return false;
    }

    switch (op) {
    case OP_EQ:
        return field->cmp(operand_field) == 0;
    case OP_NE:
        return field->cmp(operand_field) != 0;
    case OP_LT:
        return field->cmp(operand_field) < 0;
    case OP_LE:
        return field->cmp(operand_field) <= 0;
    case OP_GT:
        return field->cmp(operand_field) > 0;
    case OP_GE:
        return field->cmp(operand_field) >= 0;
    case OP_IN:
        return operand_set.find(field) != operand_set.end();
    case OP_IS: {
        if (operand_field->is_null() == field->is_null()) {
            return true;
        } else {
            return false;
        }
    }
    default:
        // Unknown operation type, just return false
        return false;
    }
}

bool Cond::eval(const ColumnStatistics& statistic) const {
    //通过单列上的单个查询条件对block进行过滤。
    if (statistic.ignored()) {
        return true;
    }

    if (OP_IS != op && statistic.minimum()->is_null()) {
        return true;
    }

    switch (op) {
    case OP_EQ: {
        return operand_field->cmp(statistic.minimum()) >= 0
               && operand_field->cmp(statistic.maximum()) <= 0;
    }
    case OP_NE: {
        return operand_field->cmp(statistic.minimum()) < 0
               || operand_field->cmp(statistic.maximum()) > 0;
    }
    case OP_LT: {
        return operand_field->cmp(statistic.minimum()) > 0;
    }
    case OP_LE: {
        return operand_field->cmp(statistic.minimum()) >= 0;
    }
    case OP_GT: {
        return operand_field->cmp(statistic.maximum()) < 0;
    }
    case OP_GE: {
        return operand_field->cmp(statistic.maximum()) <= 0;
    }
    case OP_IN: {
        FieldSet::const_iterator it = operand_set.begin();
        for (; it != operand_set.end(); ++it) {
            if ((*it)->cmp(statistic.minimum()) >= 0 
                    && (*it)->cmp(statistic.maximum()) <= 0) {
                return true;
            }
        }
        break;
    }
    case OP_IS: {
        if (operand_field->is_null()) {
            if (statistic.minimum()->is_null()) {
                return true;
            } else {
                return false;
            }
        } else {
            if (!statistic.maximum()->is_null()) {
                return true;
            } else {
                return false;
            }
        }
    }
    default:
        break;
    }

    return false;
}

int Cond::del_eval(const ColumnStatistics& stat) const {
    //通过单列上的单个删除条件对block进行过滤。
    if (stat.ignored()) {
        //for string type, the column statistics may be not recorded in block level
        //so it can be ignored for ColumnStatistics.
        return DEL_PARTIAL_SATISFIED;
    }

    if (OP_IS != op) {
        if (stat.minimum()->is_null() && stat.maximum()->is_null()) {
            return DEL_NOT_SATISFIED;
        } else if (stat.minimum()->is_null() && !stat.maximum()->is_null()) {
            return DEL_PARTIAL_SATISFIED;
        }
    }

    int ret = DEL_NOT_SATISFIED;
    switch (op) {
    case OP_EQ: {
        if (operand_field->cmp(stat.minimum()) == 0
            && operand_field->cmp(stat.maximum()) == 0){
            ret = DEL_SATISFIED;
        } else if (operand_field->cmp(stat.minimum()) >= 0
            && operand_field->cmp(stat.maximum()) <= 0) {
            ret = DEL_PARTIAL_SATISFIED;
        } else {
            ret = DEL_NOT_SATISFIED;
        }
        return ret;
    }
    case OP_NE: {
        if (operand_field->cmp(stat.minimum()) == 0
            && operand_field->cmp(stat.maximum()) == 0) {
            ret = DEL_NOT_SATISFIED;
        } else if (operand_field->cmp(stat.minimum()) >= 0
            && operand_field->cmp(stat.maximum()) <= 0) {
            ret = DEL_PARTIAL_SATISFIED;
        } else {
            ret = DEL_SATISFIED;
        }
        return ret;
    }
    case OP_LT: {
        if (operand_field->cmp(stat.minimum()) <= 0) {
            ret = DEL_NOT_SATISFIED;
        } else if (operand_field->cmp(stat.maximum()) > 0) {
            ret = DEL_SATISFIED;
        } else {
            ret = DEL_PARTIAL_SATISFIED;
        }
        return ret;
    }
    case OP_LE: {
        if (operand_field->cmp(stat.minimum()) < 0) {
            ret = DEL_NOT_SATISFIED;
        } else if (operand_field->cmp(stat.maximum()) >= 0) {
            ret = DEL_SATISFIED;
        } else {
            ret = DEL_PARTIAL_SATISFIED;
        }
        return ret;
    }
    case OP_GT: {
        if (operand_field->cmp(stat.maximum()) >= 0) {
            ret = DEL_NOT_SATISFIED;
        } else if (operand_field->cmp(stat.minimum()) < 0) {
            ret = DEL_SATISFIED;
        } else {
            ret = DEL_PARTIAL_SATISFIED;
        }
        return ret;
    }
    case OP_GE: {
        if (operand_field->cmp(stat.maximum()) > 0) {
            ret = DEL_NOT_SATISFIED;
        } else if (operand_field->cmp(stat.minimum()) <= 0) {
            ret = DEL_SATISFIED;
        } else {
            ret = DEL_PARTIAL_SATISFIED;
        }
        return ret;
    }
    case OP_IN: {
        //IN和OR等价，只要有一个操作数满足删除条件就可以全部过滤；
        //有一个部分满足删除条件，就可以部分过滤
        FieldSet::const_iterator it = operand_set.begin();
        for (; it != operand_set.end(); ++it) {
            if ((*it)->cmp(stat.minimum()) >= 0
                && (*it)->cmp(stat.maximum()) <= 0) {
                if (stat.minimum()->cmp(stat.maximum()) == 0) {
                    ret = DEL_SATISFIED;
                } else {
                    ret = DEL_PARTIAL_SATISFIED;
                }
                break;
            }
        }
        if (it == operand_set.end()) {
            ret = DEL_NOT_SATISFIED;
        }
        return ret;
    }
    case OP_IS: {
        if (operand_field->is_null()) {
            if (stat.minimum()->is_null() && stat.maximum()->is_null()) {
                ret = DEL_SATISFIED;
            } else if (stat.minimum()->is_null() && !stat.maximum()->is_null()) {
                ret = DEL_PARTIAL_SATISFIED;
            } else {
                //不会出现min不为NULL，max为NULL
                ret = DEL_NOT_SATISFIED;
            }
        } else {
            if (stat.minimum()->is_null() && stat.maximum()->is_null()) {
                ret = DEL_NOT_SATISFIED;
            } else if (stat.minimum()->is_null() && !stat.maximum()->is_null()) {
                ret = DEL_PARTIAL_SATISFIED;
            } else {
                ret = DEL_SATISFIED;
            }
        }
        return ret;
    }
    default:
        break;
    }
    return ret;
}

bool Cond::eval(const std::pair<Field *, Field *>& statistic) const {
    //通过单列上的单个查询条件对version进行过滤
    if (OP_IS != op && statistic.first->is_null()) {
        return true;
    }
    switch (op) {
    case OP_EQ: {
        return operand_field->cmp(statistic.first) >= 0
               && operand_field->cmp(statistic.second) <= 0;
    }
    case OP_NE: {
        return operand_field->cmp(statistic.first) < 0
               || operand_field->cmp(statistic.second) > 0;
    }
    case OP_LT: {
        return operand_field->cmp(statistic.first) > 0;
    }
    case OP_LE: {
        return operand_field->cmp(statistic.first) >= 0;
    }
    case OP_GT: {
        return operand_field->cmp(statistic.second) < 0;
    }
    case OP_GE: {
        return operand_field->cmp(statistic.second) <= 0;
    }
    case OP_IN: {
        FieldSet::const_iterator it = operand_set.begin();
        for (; it != operand_set.end(); ++it) {
            if ((*it)->cmp(statistic.first) >= 0 
                    && (*it)->cmp(statistic.second) <= 0) {
                return true;
            }
        }
        break;
    }
    case OP_IS: {
        if (operand_field->is_null()) {
            if (statistic.first->is_null()) {
                return true;
            } else {
                return false;
            }
        } else {
            if (!statistic.second->is_null()) {
                return true;
            } else {
                return false;
            }
        }
    }
    default:
        break;
    }

    return false;
}

int Cond::del_eval(const std::pair<Field *, Field *>& stat) const {
    //通过单列上的单个删除条件对version进行过滤。
    if (OP_IS != op) {
        if (stat.first->is_null() && stat.second->is_null()) {
            return DEL_NOT_SATISFIED;
        } else if (stat.first->is_null() && !stat.second->is_null()) {
            return DEL_PARTIAL_SATISFIED;
        }
    }

    int ret = DEL_NOT_SATISFIED;
    switch (op) {
    case OP_EQ: {
        if (operand_field->cmp(stat.first) == 0
            && operand_field->cmp(stat.second) == 0){
            ret = DEL_SATISFIED;
        } else if (operand_field->cmp(stat.first) >= 0
            && operand_field->cmp(stat.second) <= 0) {
            ret = DEL_PARTIAL_SATISFIED;
        } else {
            ret = DEL_NOT_SATISFIED;
        }
        return ret;
    }
    case OP_NE: {
        if (operand_field->cmp(stat.first) == 0
            && operand_field->cmp(stat.second) == 0) {
            ret = DEL_NOT_SATISFIED;
        } else if (operand_field->cmp(stat.first) >= 0
            && operand_field->cmp(stat.second) <= 0) {
            ret = DEL_PARTIAL_SATISFIED;
        } else {
            ret = DEL_SATISFIED;
        }
        return ret;
    }
    case OP_LT: {
        if (operand_field->cmp(stat.first) <= 0) {
            ret = DEL_NOT_SATISFIED;
        } else if (operand_field->cmp(stat.second) > 0) {
            ret = DEL_SATISFIED;
        } else {
            ret = DEL_PARTIAL_SATISFIED;
        }
        return ret;
    }
    case OP_LE: {
        if (operand_field->cmp(stat.first) < 0) {
            ret = DEL_NOT_SATISFIED;
        } else if (operand_field->cmp(stat.second) >= 0) {
            ret = DEL_SATISFIED;
        } else {
            ret = DEL_PARTIAL_SATISFIED;
        }
        return ret;
    }
    case OP_GT: {
        if (operand_field->cmp(stat.second) >= 0) {
            ret = DEL_NOT_SATISFIED;
        } else if (operand_field->cmp(stat.first) < 0) {
            ret = DEL_SATISFIED;
        } else {
            ret = DEL_PARTIAL_SATISFIED;
        }
        return ret;
    }
    case OP_GE: {
        if (operand_field->cmp(stat.second) > 0) {
            ret = DEL_NOT_SATISFIED;
        } else if (operand_field->cmp(stat.first) <= 0) {
            ret = DEL_SATISFIED;
        } else {
            ret = DEL_PARTIAL_SATISFIED;
        }
        return ret;
    }
    case OP_IN: {
        FieldSet::const_iterator it = operand_set.begin();
        for (; it != operand_set.end(); ++it) {
            if ((*it)->cmp(stat.first) >= 0
                && (*it)->cmp(stat.second) <= 0) {
                if (stat.first->cmp(stat.second) == 0) {
                    ret = DEL_SATISFIED;
                } else {
                    ret = DEL_PARTIAL_SATISFIED;
                }
                break;
            }
        }
        if (it == operand_set.end()) {
            ret = DEL_SATISFIED;
        }
        return ret;
    }
    case OP_IS: {
        if (operand_field->is_null()) {
            if (stat.first->is_null() && stat.second->is_null()) {
                ret = DEL_SATISFIED;
            } else if (stat.first->is_null() && !stat.second->is_null()) {
                ret = DEL_PARTIAL_SATISFIED;
            } else {
                //不会出现min不为NULL，max为NULL
                ret = DEL_NOT_SATISFIED;
            }
        } else {
            if (stat.first->is_null() && stat.second->is_null()) {
                ret = DEL_NOT_SATISFIED;
            } else if (stat.first->is_null() && !stat.second->is_null()) {
                ret = DEL_PARTIAL_SATISFIED;
            } else {
                ret = DEL_SATISFIED;
            }
        }
        return ret;
    }
    default:
        break;
    }
    return ret;
}

bool Cond::eval(const column_file::BloomFilter& bf) const {
    //通过单列上BloomFilter对block进行过滤。
    switch (op) {
    case OP_EQ: {
        return bf.test_bytes(operand_field->buf(), operand_field->size());
    }
    case OP_IN: {
        FieldSet::const_iterator it = operand_set.begin();
        for (; it != operand_set.end(); ++it) {
            if (bf.test_bytes((*it)->buf(), (*it)->size())) {
                return true;
            }
        }
        return false;
    }
    default:
        break;
    }

    return false;
}

Field* Cond::create_field(const FieldInfo& fi) {
    return create_field(fi, 0);
}

Field* Cond::create_field(const FieldInfo& fi, uint32_t len) {
    Field* f = Field::create(fi);
    if (f == NULL) {
        OLAP_LOG_WARNING("fail to create Field Object. [type=%d]", fi.type);
        return NULL;
    }

    uint32_t buf_len = 0;
    switch (fi.type) {
        case OLAP_FIELD_TYPE_VARCHAR:
            buf_len = std::max((uint32_t)(len + sizeof(VarCharField::LengthValueType)),
                                  fi.length);
            f->set_buf_size(buf_len);
            f->set_string_length(buf_len);
            break;
        case OLAP_FIELD_TYPE_CHAR:
            buf_len = std::max(len,
                               fi.length);
            f->set_buf_size(buf_len);
            f->set_string_length(buf_len);
            break;
        default:
            buf_len = fi.length;
    } 

    char* buf = new(nothrow) char[buf_len + sizeof(char)];
    memset(buf, 0, buf_len + sizeof(char));
    if (buf == NULL) {
        OLAP_LOG_WARNING("fail to alloc memory for field::attach. [length=%u]", buf_len);
        return NULL;
    }

    operand_field_buf.push_back(buf);
    f->attach_field(buf);

    return f;
}

CondColumn::CondColumn(const CondColumn& from) {
    for (vector<Cond>::const_iterator it = from._conds.begin(); it != from._conds.end(); ++it) {
        _conds.push_back(*it);
    }

    _table = from._table;
    _is_key = from._is_key;
    _col_index = from._col_index;
}

// PRECONDITION 1. index is valid; 2. at least has one operand
bool CondColumn::add_condition(Cond* condition) {
    if (condition->op == OP_IS) {
        OLAP_LOG_DEBUG("Use cond.operand_field to initialize Field Object."
                       "[for_column=%d; operand=%s; op_type=%d]",
                       _col_index, condition->operands.begin()->c_str(), condition->op);
        Field* f = condition->create_field(_table->tablet_schema()[_col_index],
                                            condition->operands.begin()->length());
        if (f == NULL) {
            return false;
        }

        if (0 == strcasecmp(condition->operands.begin()->c_str(), "NULL")) {
            f->set_null();
        } else {
            f->set_not_null();
        }
        condition->operand_field = f;
    } else if (condition->op != OP_IN) {
        OLAP_LOG_DEBUG("Use cond.operand_field to initialize Field Object."
                       "[for_column=%d; operand=%s; op_type=%d]",
                       _col_index, condition->operands.begin()->c_str(), condition->op);

        Field* f = condition->create_field(_table->tablet_schema()[_col_index],
                                           condition->operands.begin()->length());
        if (f == NULL) {
            return false;
        }

        if (OLAP_SUCCESS != f->from_string(*(condition->operands.begin()))) {
            return false;
        }

        condition->operand_field = f;
    } else {
        for (vector<string>::iterator it = condition->operands.begin();
                it != condition->operands.end(); ++it) {
            OLAP_LOG_DEBUG("Use cond.operand_set to initialize Field Objects."
                           "[for_column=%d; operands=%s]", _col_index, it->c_str());

            Field* f = condition->create_field(_table->tablet_schema()[_col_index],
                                               it->length());
              
            if (f == NULL) {
                return false;
            }

            if (OLAP_SUCCESS != f->from_string(*it)) {
                return false;
            }

            pair<Cond::FieldSet::iterator, bool> insert_reslut = 
                    condition->operand_set.insert(f);
            if (!insert_reslut.second) {
                OLAP_LOG_WARNING("fail to insert operand set.[condition=%s]", it->c_str());
                SAFE_DELETE(f);
            }
        }
    }

    _conds.push_back(*condition);

    return true;
}

bool CondColumn::eval(const RowCursor& row) const {
    //通过一列上的所有查询条件对单行数据进行过滤
    const Field* field = row.get_field_by_index(_col_index);
    vector<Cond>::const_iterator each_cond = _conds.begin();
    for (; each_cond != _conds.end(); ++each_cond) {
        // As long as there is one condition not satisfied, we can return false
        if (!each_cond->eval(field)) {
            return false;
        }
    }

    return true;
}

bool CondColumn::eval(const ColumnStatistics& statistic) const {
    //通过一列上的所有查询条件对block进行过滤
    vector<Cond>::const_iterator each_cond = _conds.begin();
    for (; each_cond != _conds.end(); ++each_cond) {
        if (!each_cond->eval(statistic)) {
            return false;
        }
    }

    return true;
}

int CondColumn::del_eval(const ColumnStatistics& col_stat) const {
    //通过一列上的所有删除条件对block进行过滤
    int ret = DEL_NOT_SATISFIED;
    bool del_partial_stastified = false;
    bool del_not_stastified = false;

    vector<Cond>::const_iterator each_cond = _conds.begin();
    for (; each_cond != _conds.end(); ++each_cond) {
        int del_ret = each_cond->del_eval(col_stat);
        if (DEL_SATISFIED == del_ret) {
            continue;
        } else if (DEL_PARTIAL_SATISFIED == del_ret) {
            del_partial_stastified = true;
        } else {
            del_not_stastified = true;
            break;
        }
    }
    if (true == del_not_stastified || 0 == _conds.size()) {
        ret = DEL_NOT_SATISFIED;
    } else if (true == del_partial_stastified) {
        ret = DEL_PARTIAL_SATISFIED;
    } else {
        ret = DEL_SATISFIED;
    }
    return ret;

}

bool CondColumn::eval(const std::pair<Field *, Field *> &statistic) const {
    //通过一列上的所有查询条件对version进行过滤
    vector<Cond>::const_iterator each_cond = _conds.begin();
    for (; each_cond != _conds.end(); ++each_cond) {
        if (!each_cond->eval(statistic)) {
            return false;
        }
    }

    return true;
}

int CondColumn::del_eval(const std::pair<Field *, Field *>& statistic) const {
    //通过一列上的所有删除条件对version进行过滤

    /*
     * the relationship between cond A and B is A & B.
     * if all delete condition is satisfied, the data can be filtered.
     * elseif any delete condition is not satifsified, the data can't be filtered.
     * else is the partial satisfied.
    */
    int ret = DEL_NOT_SATISFIED;
    bool del_partial_statified = false;
    bool del_not_statified = false; 
    vector<Cond>::const_iterator each_cond = _conds.begin();
    for (; each_cond != _conds.end(); ++each_cond) {
        int del_ret = each_cond->del_eval(statistic);
        if (DEL_SATISFIED == del_ret) {
            continue;
        } else if (DEL_PARTIAL_SATISFIED == del_ret) {
            del_partial_statified = true;
        } else {
            del_not_statified = true;
            break;
        }
    }
    if (true == del_not_statified || 0 == _conds.size()) {
        // if the size of condcolumn vector is zero,
        // the delete condtion is not satisfied.
        ret = DEL_NOT_SATISFIED;
    } else if (true == del_partial_statified) {
        ret = DEL_PARTIAL_SATISFIED;
    } else {
        ret = DEL_SATISFIED;
    }

    return ret;
}

bool CondColumn::eval(const column_file::BloomFilter& bf) const {
    //通过一列上的所有BloomFilter索引信息对block进行过滤
    vector<Cond>::const_iterator each_cond = _conds.begin();
    for (; each_cond != _conds.end(); ++each_cond) {
        if (!each_cond->eval(bf)) {
            return false;
        }
    }

    return true;
}

void CondColumn::finalize() {
    for (vector<Cond>::iterator it = _conds.begin(); it != _conds.end(); ++it) {
        it->finalize();
    }
}

OLAPStatus Conditions::append_condition(const TCondition& condition) {
    if (_table == NULL) {
        OLAP_LOG_WARNING("fail to parse condition without any table attached. [condition=%s]",
                         apache::thrift::ThriftDebugString(condition).c_str());
        return OLAP_ERR_NOT_INITED;
    }

    // Parse triplet for condition
    Cond cond(condition);
    if (!cond.validation()) {
        OLAP_LOG_WARNING("fail to parse condition, invalid condition format. [condition=%s]",
                         apache::thrift::ThriftDebugString(condition).c_str());
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    int32_t index = _table->get_field_index(cond.column_name);
    if (index < 0) {
        OLAP_LOG_WARNING("fail to get field index, name is invalid. [index=%d; field_name=%s]",
                         index,
                         cond.column_name.c_str());
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // Skip column which is non-key, or whose type is string or float
    FieldInfo fi = _table->tablet_schema()[index];
    if (fi.type == OLAP_FIELD_TYPE_DOUBLE || fi.type == OLAP_FIELD_TYPE_FLOAT) {
        return OLAP_SUCCESS;
    }

    if (_columns.count(index) != 0) {
        if (!_columns[index].add_condition(&cond)) {
            OLAP_LOG_WARNING("fail to add condition for column. [field_index=%d]", index);
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
    } else {
        CondColumn cc(_table, index);
        if (!cc.add_condition(&cond)) {
            OLAP_LOG_WARNING("fail to add condition for column. [field_index=%d]", index);
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }

        _columns[index] = cc;
    }

    return OLAP_SUCCESS;
}

bool Conditions::delete_conditions_eval(const RowCursor& row) const {
    //通过所有列上的删除条件对rowcursor进行过滤
    if (_columns.empty()) {
        return false;
    }
    
    for (CondColumns::const_iterator each_cond = _columns.begin();
            each_cond != _columns.end(); ++each_cond) {
        if (each_cond->second.is_key() && !each_cond->second.eval(row)) {
            return false;
        }
    }

    OLAP_LOG_DEBUG("Row meets the delete conditions. [condition_count=%zu; row=%s]",
                   _columns.size(),
                   row.to_string().c_str());

    return true;
}

bool Conditions::delta_pruning_filter(
        std::vector<std::pair<Field *, Field *>> &column_statistics) const {
    //通过所有列上的删除条件对version进行过滤
    for (CondColumns::const_iterator cond_it = _columns.begin(); 
            cond_it != _columns.end(); ++cond_it) {
        if (cond_it->second.is_key() && cond_it->first > column_statistics.size()) {
            OLAP_LOG_WARNING("where condition not equal column statistics size."
                    "[cond_id=%d, column_statistics_size=%lu]", 
                    cond_it->first,
                    column_statistics.size());
            return false;
        }
        if (cond_it->second.is_key() && !cond_it->second.eval(column_statistics[cond_it->first])) {
            return true;
        }
    }
    return false;
}

int Conditions::delete_pruning_filter(
        std::vector<std::pair<Field *, Field *>> & col_stat) const {

    //通过所有列上的删除条件对version进行过滤
    /*
     * the relationship between condcolumn A and B is A & B.
     * if any delete condition is not satisfied, the data can't be filtered.
     * elseif all delete condition is satifsified, the data can be filtered.
     * else is the partial satisfied.
    */
    int ret = DEL_NOT_SATISFIED;
    bool del_partial_satisfied = false;
    bool del_not_satisfied = false;
    CondColumns::const_iterator cond_it = _columns.begin();
    for (; cond_it != _columns.end(); ++cond_it) {
        /*
         * this is base on the assumption that the delete condition
         * is only about key field, not about value field.
        */
        if (cond_it->second.is_key() && cond_it->first > col_stat.size()) {
            OLAP_LOG_WARNING("where condition not equal column statistics size."
                    "[cond_id=%d, column_statistics_size=%lu]", 
                    cond_it->first,
                    col_stat.size());
            del_partial_satisfied = true;
            continue;
        }

        std::pair<Field*, Field*> stat = col_stat[cond_it->first]; 
        int del_ret = cond_it->second.del_eval(stat);
        if (DEL_SATISFIED == del_ret) {
            continue;
        } else if (DEL_PARTIAL_SATISFIED == del_ret) {
            del_partial_satisfied = true;
        } else {
            del_not_satisfied = true;
            break;
        }
    }

    if (true == del_not_satisfied || 0 == _columns.size()) {
        // if the size of condcolumn vector is zero, 
        // the delete condtion is not satisfied.
        ret = DEL_NOT_SATISFIED;
    } else if (true == del_partial_satisfied) {
        ret = DEL_PARTIAL_SATISFIED;
    } else {
        ret = DEL_SATISFIED;
    }
    return ret;
}

}  // namespace palo

