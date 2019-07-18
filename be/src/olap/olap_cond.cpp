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

#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/utils.h"
#include "olap/wrapper_field.h"

using std::nothrow;
using std::pair;
using std::string;
using std::vector;

using doris::ColumnStatistics;

//此文件主要用于对用户发送的查询条件和删除条件进行处理，逻辑上二者都可以分为三层
//Condtiion->Condcolumn->Cond
//Condition表示用户发的单个条件
//Condcolumn表示一列上所有条件的集合。
//Conds表示一列上的单个条件.
//对于查询条件而言，各层级的条件之间都是逻辑与的关系
//对于delete条件则有不同。Cond和Condcolumn之间是逻辑与的关系，而Condtion之间是逻辑或的关系。

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
//  3. 过滤version实在Reader里面,调用rowset_pruning_filter

namespace doris {

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

Cond::Cond() : op(OP_NULL), operand_field(nullptr) {
}

Cond::~Cond() {
    delete operand_field;
    for (auto& it : operand_set) {
        delete it;
    }
}

OLAPStatus Cond::init(const TCondition& tcond, const TabletColumn& column) {
    // Parse op type
    op = parse_op_type(tcond.condition_op);
    if (op == OP_NULL || (op != OP_IN && tcond.condition_values.size() != 1)) {
        OLAP_LOG_WARNING("Condition op type is invalid. [name=%s, op=%d, size=%d]",
                         tcond.column_name.c_str(), op, tcond.condition_values.size());
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }
    if (op == OP_IS) {
        // 'is null' or 'is not null'
        auto operand = tcond.condition_values.begin();
        std::unique_ptr<WrapperField> f(WrapperField::create(column, operand->length()));
        if (f == nullptr) {
            OLAP_LOG_WARNING("Create field failed. [name=%s, operand=%s, op_type=%d]",
                             tcond.column_name.c_str(), operand->c_str(), op);
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
        if (strcasecmp(operand->c_str(), "NULL") == 0) {
            f->set_null();
        } else {
            f->set_not_null();
        }
        operand_field = f.release();
    } else if (op != OP_IN) {
        auto operand = tcond.condition_values.begin();
        std::unique_ptr<WrapperField> f(WrapperField::create(column, operand->length()));
        if (f == nullptr) {
            OLAP_LOG_WARNING("Create field failed. [name=%s, operand=%s, op_type=%d]",
                             tcond.column_name.c_str(), operand->c_str(), op);
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
        OLAPStatus res = f->from_string(*operand);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("Create field failed. [name=%s, operand=%s, op_type=%d]",
                             tcond.column_name.c_str(), operand->c_str(), op);
            return res;
        }
        operand_field = f.release();
    } else {
        for (auto& operand : tcond.condition_values) {
            std::unique_ptr<WrapperField> f(WrapperField::create(column, operand.length()));
            if (f == NULL) {
                OLAP_LOG_WARNING("Create field failed. [name=%s, operand=%s, op_type=%d]",
                                 tcond.column_name.c_str(), operand.c_str(), op);
                return OLAP_ERR_INPUT_PARAMETER_ERROR;
            }
            OLAPStatus res = f->from_string(operand);
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("Create field failed. [name=%s, operand=%s, op_type=%d]",
                                 tcond.column_name.c_str(), operand.c_str(), op);
                return res;
            }
            auto insert_reslut = operand_set.insert(f.get());
            if (!insert_reslut.second) {
                LOG(WARNING) << "Duplicate operand in in-predicate.[condition=" << operand << "]";
                // Duplicated, let unique_ptr delete field
            } else {
                // Normal case, release this unique_ptr
                f.release();
            }
        }
    }

    return OLAP_SUCCESS;
}

bool Cond::eval(char* right) const {
    //通过单列上的单个查询条件对row进行过滤
    if (right == NULL) {
        return false;
    }
    if (*reinterpret_cast<bool*>(right) && op != OP_IS) {
        //任何operand和NULL的运算都是false
        return false;
    }

    switch (op) {
    case OP_EQ:
        return operand_field->cmp(right) == 0;
    case OP_NE:
        return operand_field->cmp(right) != 0;
    case OP_LT:
        return operand_field->cmp(right) > 0;
    case OP_LE:
        return operand_field->cmp(right) >= 0;
    case OP_GT:
        return operand_field->cmp(right) < 0;
    case OP_GE:
        return operand_field->cmp(right) <= 0;
    case OP_IN: {
        for (const WrapperField* field : operand_set) {
            if (field->cmp(right) == 0) {
                return true;
            }
        }
        return false;
    }
    case OP_IS: {
        if (operand_field->is_null() == *reinterpret_cast<bool*>(right)) {
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

bool Cond::eval(const std::pair<WrapperField*, WrapperField*>& statistic) const {
    //通过单列上的单个查询条件对version进行过滤
    // When we apply column statistic, Field can be NULL when type is Varchar,
    // we just ignore this cond
    if (statistic.first == nullptr || statistic.second == nullptr) {
        return true;
    }
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

int Cond::del_eval(const std::pair<WrapperField*, WrapperField*>& stat) const {
    //通过单列上的单个删除条件对version进行过滤。
    
    // When we apply column statistics, stat maybe null.
    if (stat.first == nullptr || stat.second == nullptr) {
        //for string type, the column statistics may be not recorded in block level
        //so it can be ignored for ColumnStatistics.
        return DEL_PARTIAL_SATISFIED;
    }

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

bool Cond::eval(const BloomFilter& bf) const {
    //通过单列上BloomFilter对block进行过滤。
    switch (op) {
    case OP_EQ: {
        bool existed = false;
        if (operand_field->is_string_type()) {
            Slice* slice = (Slice*)(operand_field->ptr());
            existed = bf.test_bytes(slice->data, slice->size);
        } else {
            existed = bf.test_bytes(operand_field->ptr(), operand_field->size());
        }
        return existed;
    }
    case OP_IN: {
        FieldSet::const_iterator it = operand_set.begin();
        for (; it != operand_set.end(); ++it) {
            bool existed = false;
            if ((*it)->is_string_type()) {
                Slice* slice = (Slice*)((*it)->ptr());
                existed = bf.test_bytes(slice->data, slice->size);
            } else {
                existed = bf.test_bytes((*it)->ptr(), (*it)->size());
            }
            if (existed) { return true; }
        }
        return false;
    }
    case OP_IS: {
        // IS [NOT] NULL can only used in to filter IS NULL predicate.
        if (operand_field->is_null()) {
            return bf.test_bytes(nullptr, 0);
        }
    }
    default:
        break;
    }

    return false;
}

CondColumn::~CondColumn() {
    for (auto& it : _conds) {
        delete it;
    }
}

// PRECONDITION 1. index is valid; 2. at least has one operand
OLAPStatus CondColumn::add_cond(const TCondition& tcond, const TabletColumn& column) {
    std::unique_ptr<Cond> cond(new Cond());
    auto res = cond->init(tcond, column);
    if (res != OLAP_SUCCESS) {
        return res;
    }
    _conds.push_back(cond.release());
    return OLAP_SUCCESS;
}

bool CondColumn::eval(const RowCursor& row) const {
    //通过一列上的所有查询条件对单行数据进行过滤
    Field* field = const_cast<Field*>(row.get_field_by_index(_col_index));
    char* buf = field->get_field_ptr(row.get_buf());
    for (auto& each_cond : _conds) {
        // As long as there is one condition not satisfied, we can return false
        if (!each_cond->eval(buf)) {
            return false;
        }
    }

    return true;
}

bool CondColumn::eval(const std::pair<WrapperField*, WrapperField*> &statistic) const {
    //通过一列上的所有查询条件对version进行过滤
    for (auto& each_cond : _conds) {
        if (!each_cond->eval(statistic)) {
            return false;
        }
    }

    return true;
}

int CondColumn::del_eval(const std::pair<WrapperField*, WrapperField*>& statistic) const {
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
    for (auto& each_cond : _conds) {
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

bool CondColumn::eval(const BloomFilter& bf) const {
    //通过一列上的所有BloomFilter索引信息对block进行过滤
    for (auto& each_cond : _conds) {
        if (!each_cond->eval(bf)) {
            return false;
        }
    }

    return true;
}

OLAPStatus Conditions::append_condition(const TCondition& tcond) {
    int32_t index = _get_field_index(tcond.column_name);
    if (index < 0) {
        LOG(WARNING) << "fail to get field index, name is invalid. index=" << index
                     << ", field_name=" << tcond.column_name;
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // Skip column which is non-key, or whose type is string or float
    const TabletColumn& column = _schema->column(index);
    if (column.type() == OLAP_FIELD_TYPE_DOUBLE
            || column.type() == OLAP_FIELD_TYPE_FLOAT) {
        return OLAP_SUCCESS;
    }

    CondColumn* cond_col = nullptr;
    auto it = _columns.find(index);
    if (it == _columns.end()) {
        cond_col = new CondColumn(*_schema, index);
        _columns[index] = cond_col;
    } else {
        cond_col = it->second;
    }

    return cond_col->add_cond(tcond, column);
}

bool Conditions::delete_conditions_eval(const RowCursor& row) const {
    //通过所有列上的删除条件对rowcursor进行过滤
    if (_columns.empty()) {
        return false;
    }
    
    for (auto& each_cond : _columns) {
        if (each_cond.second->is_key() && !each_cond.second->eval(row)) {
            return false;
        }
    }

    VLOG(3) << "Row meets the delete conditions. "
            << "condition_count=" << _columns.size()
            << ", row=" << row.to_string();
    return true;
}

bool Conditions::rowset_pruning_filter(const std::vector<KeyRange>& zone_maps) const {
    //通过所有列上的删除条件对version进行过滤
    for (auto& cond_it : _columns) {
        if (cond_it.second->is_key() && cond_it.first > zone_maps.size()) {
            LOG(WARNING) << "where condition not equal zone maps size. "
                         << "cond_id=" << cond_it.first
                         << ", zone_map_size=" << zone_maps.size();
            return false;
        }
        if (cond_it.second->is_key() && !cond_it.second->eval(zone_maps[cond_it.first])) {
            return true;
        }
    }
    return false;
}

int Conditions::delete_pruning_filter(const std::vector<KeyRange>& zone_maps) const {
    if (_columns.empty()) {
        return DEL_NOT_SATISFIED;
    }
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
    for (auto& cond_it : _columns) {
        /*
         * this is base on the assumption that the delete condition
         * is only about key field, not about value field.
        */
        if (cond_it.second->is_key() && cond_it.first > zone_maps.size()) {
            LOG(WARNING) << "where condition not equal column statistics size. "
                         << "cond_id=" << cond_it.first
                         << ", zone_map_size=" << zone_maps.size();
            del_partial_satisfied = true;
            continue;
        }

        int del_ret = cond_it.second->del_eval(zone_maps[cond_it.first]);
        if (DEL_SATISFIED == del_ret) {
            continue;
        } else if (DEL_PARTIAL_SATISFIED == del_ret) {
            del_partial_satisfied = true;
        } else {
            del_not_satisfied = true;
            break;
        }
    }

    if (del_not_satisfied) {
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

}  // namespace doris

