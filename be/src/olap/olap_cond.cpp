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

#include <thrift/protocol/TDebugProtocol.h>

#include <cstring>
#include <string>
#include <utility>

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
//Condition->Condcolumn->Cond
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

#define MAX_OP_STR_LENGTH 3

static CondOp parse_op_type(const string& op) {
    if (op.size() > MAX_OP_STR_LENGTH) {
        return OP_NULL;
    }

    if (op == "=") {
        return OP_EQ;
    } else if (0 == strcasecmp(op.c_str(), "is")) {
        return OP_IS;
    } else if (op == "!=") {
        return OP_NE;
    } else if (op == "*=") {
        return OP_IN;
    } else if (op == "!*=") {
        return OP_NOT_IN;
    } else if (op == ">=") {
        return OP_GE;
    } else if (op == ">>" || op == ">") {
        return OP_GT;
    } else if (op == "<=") {
        return OP_LE;
    } else if (op == "<<" || op == "<") {
        return OP_LT;
    }

    return OP_NULL;
}

Cond::~Cond() {
    delete operand_field;
    for (auto& it : operand_set) {
        delete it;
    }
    min_value_field = nullptr;
    max_value_field = nullptr;
}

OLAPStatus Cond::init(const TCondition& tcond, const TabletColumn& column) {
    // Parse op type
    op = parse_op_type(tcond.condition_op);
    if (op == OP_NULL || (op != OP_IN && op != OP_NOT_IN && tcond.condition_values.size() != 1)) {
        OLAP_LOG_WARNING("Condition op type is invalid. [name=%s, op=%d, size=%d]",
                         tcond.column_name.c_str(), op, tcond.condition_values.size());
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }
    if (op == OP_IS) {
        // 'is null' or 'is not null'
        DCHECK_EQ(tcond.condition_values.size(), 1);
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
    } else if (op != OP_IN && op != OP_NOT_IN) {
        DCHECK_EQ(tcond.condition_values.size(), 1);
        auto operand = tcond.condition_values.begin();
        std::unique_ptr<WrapperField> f(WrapperField::create(column, operand->length()));
        if (f == nullptr) {
            OLAP_LOG_WARNING("Create field failed. [name=%s, operand=%s, op_type=%d]",
                             tcond.column_name.c_str(), operand->c_str(), op);
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
        OLAPStatus res = f->from_string(*operand);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("Convert from string failed. [name=%s, operand=%s, op_type=%d]",
                             tcond.column_name.c_str(), operand->c_str(), op);
            return res;
        }
        operand_field = f.release();
    } else {
        DCHECK(op == OP_IN || op == OP_NOT_IN);
        DCHECK(!tcond.condition_values.empty());
        for (auto& operand : tcond.condition_values) {
            std::unique_ptr<WrapperField> f(WrapperField::create(column, operand.length()));
            if (f == nullptr) {
                OLAP_LOG_WARNING("Create field failed. [name=%s, operand=%s, op_type=%d]",
                                 tcond.column_name.c_str(), operand.c_str(), op);
                return OLAP_ERR_INPUT_PARAMETER_ERROR;
            }
            OLAPStatus res = f->from_string(operand);
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("Convert from string failed. [name=%s, operand=%s, op_type=%d]",
                                 tcond.column_name.c_str(), operand.c_str(), op);
                return res;
            }
            if (min_value_field == nullptr || f->cmp(min_value_field) < 0) {
                min_value_field = f.get();
            }

            if (max_value_field == nullptr || f->cmp(max_value_field) > 0) {
                max_value_field = f.get();
            }

            auto insert_result = operand_set.insert(f.get());
            if (!insert_result.second) {
                LOG(WARNING) << "Duplicate operand in in-predicate.[condition=" << operand << "]";
                // Duplicated, let std::unique_ptr delete field
            } else {
                // Normal case, release this std::unique_ptr
                f.release();
            }
        }
    }

    return OLAP_SUCCESS;
}

bool Cond::eval(const RowCursorCell& cell) const {
    if (cell.is_null() && op != OP_IS) {
        //任何非OP_IS operand和NULL的运算都是false
        return false;
    }

    switch (op) {
    case OP_EQ:
        return operand_field->field()->compare_cell(*operand_field, cell) == 0;
    case OP_NE:
        return operand_field->field()->compare_cell(*operand_field, cell) != 0;
    case OP_LT:
        return operand_field->field()->compare_cell(*operand_field, cell) > 0;
    case OP_LE:
        return operand_field->field()->compare_cell(*operand_field, cell) >= 0;
    case OP_GT:
        return operand_field->field()->compare_cell(*operand_field, cell) < 0;
    case OP_GE:
        return operand_field->field()->compare_cell(*operand_field, cell) <= 0;
    case OP_IN: {
        WrapperField wrapperField(const_cast<Field*>(min_value_field->field()), cell);
        auto ret = operand_set.find(&wrapperField) != operand_set.end();
        wrapperField.release_field();
        return ret;
    }
    case OP_NOT_IN: {
        WrapperField wrapperField(const_cast<Field*>(min_value_field->field()), cell);
        auto ret = operand_set.find(&wrapperField) == operand_set.end();
        wrapperField.release_field();
        return ret;
    }
    case OP_IS: {
        return operand_field->is_null() == cell.is_null();
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
        return operand_field->cmp(statistic.first) >= 0 &&
               operand_field->cmp(statistic.second) <= 0;
    }
    case OP_NE: {
        return true;
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
        return min_value_field->cmp(statistic.second) <= 0 &&
               max_value_field->cmp(statistic.first) >= 0;
    }
    case OP_NOT_IN: {
        return true;
    }
    case OP_IS: {
        if (operand_field->is_null()) {
            return statistic.first->is_null();
        } else {
            return !statistic.second->is_null();
        }
    }
    default:
        break;
    }

    return false;
}

int Cond::del_eval(const std::pair<WrapperField*, WrapperField*>& stat) const {
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
        int cmp1 = operand_field->cmp(stat.first);
        int cmp2 = operand_field->cmp(stat.second);
        if (cmp1 == 0 && cmp2 == 0) {
            ret = DEL_SATISFIED;
        } else if (cmp1 >= 0 && cmp2 <= 0) {
            ret = DEL_PARTIAL_SATISFIED;
        } else {
            ret = DEL_NOT_SATISFIED;
        }
        return ret;
    }
    case OP_NE: {
        int cmp1 = operand_field->cmp(stat.first);
        int cmp2 = operand_field->cmp(stat.second);
        if (cmp1 == 0 && cmp2 == 0) {
            ret = DEL_NOT_SATISFIED;
        } else if (cmp1 >= 0 && cmp2 <= 0) {
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
        if (stat.first->cmp(stat.second) == 0) {
            if (operand_set.find(stat.first) != operand_set.end()) {
                ret = DEL_SATISFIED;
            } else {
                ret = DEL_NOT_SATISFIED;
            }
        } else {
            if (min_value_field->cmp(stat.second) <= 0 && max_value_field->cmp(stat.first) >= 0) {
                ret = DEL_PARTIAL_SATISFIED;
            } else {
                ret = DEL_NOT_SATISFIED;
            }
        }
        return ret;
    }
    case OP_NOT_IN: {
        if (stat.first->cmp(stat.second) == 0) {
            if (operand_set.find(stat.first) == operand_set.end()) {
                ret = DEL_SATISFIED;
            } else {
                ret = DEL_NOT_SATISFIED;
            }
        } else {
            if (min_value_field->cmp(stat.second) > 0 || max_value_field->cmp(stat.first) < 0) {
                // When there is no intersection, all entries in the range should be deleted.
                ret = DEL_SATISFIED;
            } else {
                ret = DEL_PARTIAL_SATISFIED;
            }
        }
        return ret;
    }
    case OP_IS: {
        if (operand_field->is_null()) {
            if (stat.first->is_null() && stat.second->is_null()) {
                ret = DEL_SATISFIED;
            } else if (stat.first->is_null() && !stat.second->is_null()) {
                ret = DEL_PARTIAL_SATISFIED;
            } else if (!stat.first->is_null() && !stat.second->is_null()){
                ret = DEL_NOT_SATISFIED;
            } else {
                CHECK(false) << "It will not happen when the stat's min is not null and max is null";
            }
        } else {
            if (stat.first->is_null() && stat.second->is_null()) {
                ret = DEL_NOT_SATISFIED;
            } else if (stat.first->is_null() && !stat.second->is_null()) {
                ret = DEL_PARTIAL_SATISFIED;
            } else if (!stat.first->is_null() && !stat.second->is_null()){
                ret = DEL_SATISFIED;
            } else {
                CHECK(false) << "It will not happen when the stat's min is not null and max is null";
            }
        }
        return ret;
    }
    default:
        LOG(WARNING) << "Not supported operation: " << op;
        break;
    }
    return ret;
}

bool Cond::eval(const BloomFilter& bf) const {
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
            if (existed) {
                return true;
            }
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

    return true;
}

bool Cond::eval(const segment_v2::BloomFilter* bf) const {
    switch (op) {
    case OP_EQ: {
        bool existed = false;
        if (operand_field->is_string_type()) {
            Slice* slice = (Slice*)(operand_field->ptr());
            existed = bf->test_bytes(slice->data, slice->size);
        } else {
            existed = bf->test_bytes(operand_field->ptr(), operand_field->size());
        }
        return existed;
    }
    case OP_IN: {
        FieldSet::const_iterator it = operand_set.begin();
        for (; it != operand_set.end(); ++it) {
            bool existed = false;
            if ((*it)->is_string_type()) {
                Slice* slice = (Slice*)((*it)->ptr());
                existed = bf->test_bytes(slice->data, slice->size);
            } else {
                existed = bf->test_bytes((*it)->ptr(), (*it)->size());
            }
            if (existed) {
                return true;
            }
        }
        return false;
    }
    case OP_IS: {
        // IS [NOT] NULL can only used in to filter IS NULL predicate.
        return operand_field->is_null() == bf->test_bytes(nullptr, 0);
    }
    default:
        break;
    }

    return true;
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
    auto cell = row.cell(_col_index);
    for (auto& each_cond : _conds) {
        // As long as there is one condition not satisfied, we can return false
        if (!each_cond->eval(cell)) {
            return false;
        }
    }

    return true;
}

bool CondColumn::eval(const std::pair<WrapperField*, WrapperField*> &statistic) const {
    for (auto& each_cond : _conds) {
        // As long as there is one condition not satisfied, we can return false
        if (!each_cond->eval(statistic)) {
            return false;
        }
    }

    return true;
}

int CondColumn::del_eval(const std::pair<WrapperField*, WrapperField*>& statistic) const {
    /*
     * the relationship between cond A and B is A & B.
     * if all delete condition is satisfied, the data can be filtered.
     * elseif any delete condition is not satisfied, the data can't be filtered.
     * else is the partial satisfied.
    */
    int ret = DEL_NOT_SATISFIED;
    bool del_partial_satisfied = false;
    bool del_not_satisfied = false;
    for (auto& each_cond : _conds) {
        int del_ret = each_cond->del_eval(statistic);
        if (DEL_SATISFIED == del_ret) {
            continue;
        } else if (DEL_PARTIAL_SATISFIED == del_ret) {
            del_partial_satisfied = true;
        } else {
            del_not_satisfied = true;
            break;
        }
    }
    if (del_not_satisfied || _conds.empty()) {
        // if the size of condcolumn vector is zero,
        // the delete condtion is not satisfied.
        ret = DEL_NOT_SATISFIED;
    } else if (del_partial_satisfied) {
        ret = DEL_PARTIAL_SATISFIED;
    } else {
        ret = DEL_SATISFIED;
    }

    return ret;
}

bool CondColumn::eval(const BloomFilter& bf) const {
    for (auto& each_cond : _conds) {
        if (!each_cond->eval(bf)) {
            return false;
        }
    }

    return true;
}

bool CondColumn::eval(const segment_v2::BloomFilter* bf) const {
    for (auto& each_cond : _conds) {
        if (!each_cond->eval(bf)) {
            return false;
        }
    }

    return true;
}

OLAPStatus Conditions::append_condition(const TCondition& tcond) {
    DCHECK(_schema != nullptr);
    int32_t index = _schema->field_index(tcond.column_name);
    if (index < 0) {
        LOG(WARNING) << "fail to get field index, field name=" << tcond.column_name;
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // Skip column which is non-key, or whose type is string or float
    const TabletColumn& column = _schema->column(index);
    if (column.type() == OLAP_FIELD_TYPE_DOUBLE || column.type() == OLAP_FIELD_TYPE_FLOAT) {
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
    if (_columns.empty()) {
        return false;
    }

    for (auto& each_cond : _columns) {
        if (_cond_column_is_key_or_duplicate(each_cond.second) && !each_cond.second->eval(row)) {
            return false;
        }
    }

    VLOG_NOTICE << "Row meets the delete conditions. "
            << "condition_count=" << _columns.size() << ", row=" << row.to_string();
    return true;
}

bool Conditions::rowset_pruning_filter(const std::vector<KeyRange>& zone_maps) const {
    // ZoneMap will store min/max of rowset.
    // The function is to filter rowset using ZoneMaps
    // and query predicates.
    for (auto& cond_it : _columns) {
        if (_cond_column_is_key_or_duplicate(cond_it.second)) {
            if (cond_it.first < zone_maps.size() &&
                !cond_it.second->eval(zone_maps.at(cond_it.first))) {
                return true;
            }
        }
    }
    return false;
}

int Conditions::delete_pruning_filter(const std::vector<KeyRange>& zone_maps) const {
    if (_columns.empty()) {
        return DEL_NOT_SATISFIED;
    }
    // ZoneMap and DeletePredicate are all stored in TabletMeta.
    // This function is to filter rowset using ZoneMap and Delete Predicate.
    /*
     * the relationship between condcolumn A and B is A & B.
     * if any delete condition is not satisfied, the data can't be filtered.
     * elseif all delete condition is satisfied, the data can be filtered.
     * else is the partial satisfied.
    */
    int ret = DEL_NOT_SATISFIED;
    bool del_partial_satisfied = false;
    bool del_not_satisfied = false;
    for (auto& cond_it : _columns) {
        /*
         * this is base on the assumption that the delete condition
         * is only about key field, not about value field except the storage model is duplicate.
        */
        if (_cond_column_is_key_or_duplicate(cond_it.second) && cond_it.first > zone_maps.size()) {
            LOG(WARNING) << "where condition not equal column statistics size. "
                         << "cond_id=" << cond_it.first << ", zone_map_size=" << zone_maps.size();
            del_partial_satisfied = true;
            continue;
        }

        int del_ret = cond_it.second->del_eval(zone_maps.at(cond_it.first));
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
        ret = DEL_NOT_SATISFIED;
    } else if (del_partial_satisfied) {
        ret = DEL_PARTIAL_SATISFIED;
    } else {
        ret = DEL_SATISFIED;
    }
    return ret;
}

CondColumn* Conditions::get_column(int32_t cid) const {
    auto iter = _columns.find(cid);
    if (iter != _columns.end()) {
        return iter->second;
    }
    return nullptr;
}

} // namespace doris
