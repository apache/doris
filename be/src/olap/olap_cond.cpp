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
        std::shared_ptr<WrapperField> f(WrapperField::create(column, operand->length()));
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
        operand_field = f;
    } else if (op != OP_IN && op != OP_NOT_IN) {
        DCHECK_EQ(tcond.condition_values.size(), 1);
        auto operand = tcond.condition_values.begin();
        std::shared_ptr<WrapperField> f(WrapperField::create(column, operand->length()));
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
        operand_field = f;
    } else {
        DCHECK(op == OP_IN || op == OP_NOT_IN);
        DCHECK(!tcond.condition_values.empty());
        for (auto& operand : tcond.condition_values) {
            std::shared_ptr<WrapperField> f(WrapperField::create(column, operand.length()));
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
            if (min_value_field == nullptr || f->cmp(min_value_field.get()) < 0) {
                min_value_field = f;
            }

            if (max_value_field == nullptr || f->cmp(max_value_field.get()) > 0) {
                max_value_field = f;
            }

            auto insert_result = operand_set.insert(f);
            if (!insert_result.second) {
                LOG(WARNING) << "Duplicate operand in in-predicate.[condition=" << operand << "]";
            }
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus Cond::intersection_cond(const Cond& other) {
    DCHECK(op == other.op ||
          (op == OP_NOT_IN && other.op == OP_NE) ||
          (op == OP_NE && other.op == OP_NOT_IN) ||
          (op == OP_IN && other.op == OP_EQ) ||
          (op == OP_EQ && other.op == OP_IN) ||
          (op == OP_LT && other.op == OP_LE) ||
          (op == OP_LE && other.op == OP_LT) ||
          (op == OP_GT && other.op == OP_GE) ||
          (op == OP_GE && other.op == OP_GT)) << "op: " << op << ", other.op: " << other.op;
    switch (op) {
        case OP_EQ:
            if (other.op == OP_EQ) {
                if (operand_field->field()->compare_cell(*operand_field, *(other.operand_field)) != 0) {
                    // No intersection, all not satisfied
                    op = OP_NULL;
                    return OLAP_SUCCESS;
                }
            } else {
                DCHECK_EQ(other.op, OP_IN) << "op: " << op << ", other.op: " << other.op;
                if (other.operand_set.find(operand_field) == other.operand_set.end()) {
                    // No intersection, all not satisfied
                    op = OP_NULL;
                    return OLAP_SUCCESS;
                }
            }
            return OLAP_SUCCESS;
        case OP_NE:
            if (other.op == OP_NE) {
                int cmp = operand_field->field()->compare_cell(*operand_field, *(other.operand_field));
                if (cmp != 0) {
                    // Transfer to OP_NOT_IN if they OP_NE to two different values
                    op = OP_NOT_IN;
                    operand_set.insert(operand_field);
                    operand_set.insert(other.operand_field);
                    min_value_field = cmp < 0 ? operand_field : other.operand_field;
                    max_value_field = cmp > 0 ? operand_field : other.operand_field;
                    // Invalidate operand_field after transferring to operand_set
                    operand_field = nullptr;
                }
            } else {
                DCHECK_EQ(other.op, OP_NOT_IN) << "op: " << op << ", other.op: " << other.op;
                if (other.operand_set.size() == 1 &&
                    operand_field->field()->compare_cell(*operand_field, *(other.min_value_field)) == 0) {
                    // Do nothing if the other's only one value equal to operand_field
                    return OLAP_SUCCESS;
                }
                // Transfer to OP_NOT_IN otherwise
                op = OP_NOT_IN;
                operand_set = other.operand_set;
                min_value_field = other.min_value_field;
                max_value_field = other.max_value_field;

                if (operand_set.find(operand_field) != operand_set.end()) {
                    // Exist a same value in operand_set, do nothing but release and invalidate operand_field
                    operand_field = nullptr;
                    return OLAP_SUCCESS;
                }

                // Insert and update min & max
                operand_set.insert(operand_field);
                if (operand_field->field()->compare_cell(*operand_field, *(min_value_field)) < 0) {
                    min_value_field = operand_field;
                }
                if (operand_field->field()->compare_cell(*operand_field, *(max_value_field)) > 0) {
                    max_value_field = operand_field;
                }

                // Invalidate operand_field after inserting to operand_set
                operand_field = nullptr;
            }
            return OLAP_SUCCESS;
        case OP_LT:
        case OP_LE: {
            int cmp = operand_field->field()->compare_cell(*operand_field, *(other.operand_field));
            if (op == other.op) {
                if (cmp > 0) {
                    operand_field = other.operand_field;
                }
                return OLAP_SUCCESS;
            }
            if (cmp == 0) {
                op = OP_LT;
            }
            return OLAP_SUCCESS;
        }
        case OP_GT:
        case OP_GE: {
            int cmp = operand_field->field()->compare_cell(*operand_field, *(other.operand_field));
            if (op == other.op) {
                if (cmp < 0) {
                    operand_field = other.operand_field;
                }
                return OLAP_SUCCESS;
            }
            if (cmp == 0) {
                op = OP_GT;
            }
            return OLAP_SUCCESS;
        }
        case OP_IN:
            if (other.op == OP_IN) {
                for (auto operand = operand_set.begin(); operand != operand_set.end();) {
                    if (other.operand_set.find(*operand) == other.operand_set.end()) {
                        // Not in other's operand_set, invalidate and release it
                        operand = operand_set.erase(operand);
                    } else {
                        ++operand;
                    }
                }
                if (operand_set.empty()) {
                    // No intersection, all not satisfied
                    op = OP_NULL;
                    return OLAP_SUCCESS;
                }

                min_value_field = nullptr;
                max_value_field = nullptr;
                if (operand_set.size() == 1) {
                    // Transfer to OP_EQ
                    op = OP_EQ;
                    operand_field = *operand_set.begin();
                    operand_set.clear();
                    return OLAP_SUCCESS;
                }

                // Update min & max
                for (const auto& operand : operand_set) {
                    if (min_value_field == nullptr || operand->field()->compare_cell(*min_value_field, *(operand)) > 0) {
                        min_value_field = operand;
                    }
                    if (max_value_field == nullptr || operand->field()->compare_cell(*max_value_field, *(operand)) < 0) {
                        max_value_field = operand;
                    }
                }
            } else {
                DCHECK_EQ(other.op, OP_EQ) << "op: " << op << ", other.op: " << other.op;
                if (operand_set.find(other.operand_field) == operand_set.end()) {
                    // No intersection, all not satisfied
                    op = OP_NULL;
                    return OLAP_SUCCESS;
                }

                // Transfer to OP_EQ
                op = OP_EQ;
                operand_field = other.operand_field;

                // Invalidate
                operand_set.clear();
                min_value_field = nullptr;
                max_value_field = nullptr;
            }
            return OLAP_SUCCESS;
        case OP_NOT_IN:
            if (other.op == OP_NOT_IN) {
                // Update min & max
                if (min_value_field->field()->compare_cell(*min_value_field, *(other.min_value_field)) > 0) {
                    min_value_field = other.min_value_field;
                }
                if (max_value_field->field()->compare_cell(*max_value_field, *(other.max_value_field)) < 0) {
                    max_value_field = other.max_value_field;
                }
                // Update operand_set
                operand_set.insert(other.operand_set.begin(), other.operand_set.end());
            } else {
                DCHECK_EQ(other.op, OP_NE) << "op: " << op << ", other.op: " << other.op;
                if (operand_set.find(other.operand_field) != operand_set.end()) {
                    // Exist a same value in operand_set, do nothing but release and invalidate this operand
                    return OLAP_SUCCESS;
                }

                // Update min & max
                if (other.operand_field->field()->compare_cell(*min_value_field, *(other.operand_field)) > 0) {
                    min_value_field = other.operand_field;
                }
                if (other.operand_field->field()->compare_cell(*max_value_field, *(other.operand_field)) < 0) {
                    max_value_field = other.operand_field;
                }

                // Update operand_set
                operand_set.insert(other.operand_field);
            }
            return OLAP_SUCCESS;
        case OP_IS:
            if (operand_field->is_null() != other.operand_field->is_null()) {
                // No intersection, all not satisfied
                op = OP_NULL;
                return OLAP_SUCCESS;
            }
            return OLAP_SUCCESS;
        default:
            op = OP_ALL;;
            return OLAP_ERR_READER_INITIALIZE_ERROR;
    }
}

OLAPStatus Cond::union_cond(const Cond& other) {
    DCHECK(op == other.op ||
           (op == OP_NOT_IN && other.op == OP_NE) ||
           (op == OP_NE && other.op == OP_NOT_IN) ||
           (op == OP_IN && other.op == OP_EQ) ||
           (op == OP_EQ && other.op == OP_IN) ||
           (op == OP_LT && other.op == OP_LE) ||
           (op == OP_LE && other.op == OP_LT) ||
           (op == OP_GT && other.op == OP_GE) ||
           (op == OP_GE && other.op == OP_GT)) << "op: " << op << ", other.op: " << other.op;
    switch (op) {
        case OP_EQ:
            if (other.op == OP_EQ) {
                int cmp = operand_field->field()->compare_cell(*operand_field, *(other.operand_field));
                if (cmp != 0) {
                    // Transfer to OP_IN if they OP_EQ to two different values
                    op = OP_IN;
                    operand_set.insert(operand_field);
                    operand_set.insert(other.operand_field);
                    min_value_field = cmp < 0 ? operand_field : other.operand_field;
                    max_value_field = cmp > 0 ? operand_field : other.operand_field;
                    // Invalidate operand_field after transferring to operand_set
                    operand_field = nullptr;
                }
            } else {
                DCHECK_EQ(other.op, OP_IN) << "op: " << op << ", other.op: " << other.op;
                // Transfer to OP_IN
                op = OP_IN;
                operand_set = other.operand_set;
                min_value_field = other.min_value_field;
                max_value_field = other.max_value_field;

                if (operand_set.find(operand_field) == operand_set.end()) {
                    // Insert and update min & max
                    operand_set.insert(operand_field);
                    if (operand_field->field()->compare_cell(*operand_field, *(min_value_field)) < 0) {
                        min_value_field = operand_field;
                    }
                    if (operand_field->field()->compare_cell(*operand_field, *(max_value_field)) > 0) {
                        max_value_field = operand_field;
                    }
                }
                operand_field = nullptr;
            }
            return OLAP_SUCCESS;
        case OP_NE:
            if (other.op == OP_NE) {
                int cmp = operand_field->field()->compare_cell(*operand_field, *(other.operand_field));
                if (cmp != 0) {
                    // All satisfied
                    op = OP_ALL;
                    operand_field = nullptr;
                }
            } else {
                DCHECK_EQ(other.op, OP_NOT_IN) << "op: " << op << ", other.op: " << other.op;
                if (other.operand_set.find(operand_field) == other.operand_set.end()) {
                    // All satisfied
                    op = OP_ALL;
                    operand_field = nullptr;
                }
            }
            return OLAP_SUCCESS;
        case OP_LT:
        case OP_LE: {
            int cmp = operand_field->field()->compare_cell(*operand_field, *(other.operand_field));
            if (op == other.op) {
                if (cmp < 0) {
                    operand_field = other.operand_field;
                }
                return OLAP_SUCCESS;
            }
            if (cmp == 0) {
                op = OP_LE;
            }
            return OLAP_SUCCESS;
        }
        case OP_GT:
        case OP_GE: {
            int cmp = operand_field->field()->compare_cell(*operand_field, *(other.operand_field));
            if (op == other.op) {
                if (cmp > 0) {
                    operand_field = other.operand_field;
                }
                return OLAP_SUCCESS;
            }
            if (cmp == 0) {
                op = OP_GE;
            }
            return OLAP_SUCCESS;
        }
        case OP_IN:
            if (other.op == OP_IN) {
                for (const auto& operand : other.operand_set) {
                    if (operand_set.find(operand) == operand_set.end()) {
                        operand_set.insert(operand);
                        if (operand->field()->compare_cell(*min_value_field, *operand) > 0) {
                            min_value_field = operand;
                        }
                        if (operand->field()->compare_cell(*max_value_field, *operand) < 0) {
                            max_value_field = operand;
                        }
                    }
                }
            } else {
                DCHECK_EQ(other.op, OP_EQ) << "op: " << op << ", other.op: " << other.op;
                if (operand_set.find(other.operand_field) == operand_set.end()) {
                    operand_set.insert(other.operand_field);
                    if (other.operand_field->field()->compare_cell(*min_value_field, *(other.operand_field)) > 0) {
                        min_value_field = other.operand_field;
                    }
                    if (other.operand_field->field()->compare_cell(*max_value_field, *(other.operand_field)) < 0) {
                        max_value_field = other.operand_field;
                    }
                }
            }
            return OLAP_SUCCESS;
        case OP_NOT_IN:
            if (other.op == OP_NOT_IN) {
                for (auto operand = operand_set.begin(); operand != operand_set.end();) {
                    if (other.operand_set.find(*operand) != other.operand_set.end()) {
                        ++operand;
                    } else {
                        operand = operand_set.erase(operand);
                    }
                }
                min_value_field = nullptr;
                max_value_field = nullptr;
                if (operand_set.empty()) {
                    // All satisfied
                    op = OP_ALL;
                    return OLAP_SUCCESS;
                }

                if (operand_set.size() == 1) {
                    // Transfer to OP_NE
                    op = OP_NE;
                    operand_field = *operand_set.begin();
                    operand_set.clear();
                    return OLAP_SUCCESS;
                }

                // Update min & max
                for (const auto& operand : operand_set) {
                    if (min_value_field == nullptr || operand->field()->compare_cell(*min_value_field, *(operand)) > 0) {
                        min_value_field = operand;
                    }
                    if (max_value_field == nullptr || operand->field()->compare_cell(*max_value_field, *(operand)) < 0) {
                        max_value_field = operand;
                    }
                }
            } else {
                DCHECK_EQ(other.op, OP_NE) << "op: " << op << ", other.op: " << other.op;
                min_value_field = nullptr;
                max_value_field = nullptr;
                if (operand_set.find(other.operand_field) == operand_set.end()) {
                    // All satisfied
                    op = OP_ALL;
                    operand_set.clear();
                    return OLAP_SUCCESS;
                }

                // Transfer to OP_NE
                op = OP_NE;
                operand_field = other.operand_field;
                operand_set.clear();
            }
            return OLAP_SUCCESS;
        case OP_IS:
            if (operand_field->is_null() != other.operand_field->is_null()) {
                // All satisfied
                op = OP_ALL;
                operand_field = nullptr;
                return OLAP_SUCCESS;
            }
            return OLAP_SUCCESS;
        default:
            op = OP_ALL;;
            return OLAP_ERR_READER_INITIALIZE_ERROR;
    }
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
        auto wrapperField = std::make_shared<WrapperField>(const_cast<Field*>(min_value_field->field()), cell);
        auto ret = operand_set.find(wrapperField) != operand_set.end();
        wrapperField->release_field();
        return ret;
    }
    case OP_NOT_IN: {
        auto wrapperField = std::make_shared<WrapperField>(const_cast<Field*>(min_value_field->field()), cell);
        auto ret = operand_set.find(wrapperField) == operand_set.end();
        wrapperField->release_field();
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
        return operand_field->cmp(statistic.first) < 0 || operand_field->cmp(statistic.second) > 0;
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
        return min_value_field->cmp(statistic.second) > 0 ||
               max_value_field->cmp(statistic.first) < 0;
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
            if (operand_set.find(std::shared_ptr<WrapperField>(stat.first, [](WrapperField*){})) != operand_set.end()) {
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
            if (operand_set.find(std::shared_ptr<WrapperField>(stat.first, [](WrapperField*){})) == operand_set.end()) {
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

// PRECONDITION 1. index is valid; 2. at least has one operand
OLAPStatus CondColumn::add_cond(const TCondition& tcond, const TabletColumn& column) {
    auto cond = std::make_shared<Cond>();
    auto res = cond->init(tcond, column);
    if (res != OLAP_SUCCESS) {
        return res;
    }
    _conds.push_back(cond);
    return OLAP_SUCCESS;
}

void CondColumn::merge_cond(const CondColumn& cond_col) {
    DCHECK_EQ(_is_key, cond_col._is_key);
    DCHECK_EQ(_col_index, cond_col._col_index);

    for (auto& cond1 : _conds) {
        for (const auto& cond2 : cond_col._conds) {
            if ((cond1->op == cond2->op) ||
                (cond1->op == OP_NOT_IN && cond2->op == OP_NE) ||
                (cond1->op == OP_NE && cond2->op == OP_NOT_IN) ||
                (cond1->op == OP_IN && cond2->op == OP_EQ) ||
                (cond1->op == OP_EQ && cond2->op == OP_IN) ||
                (cond1->op == OP_LT && cond2->op == OP_LE) ||
                (cond1->op == OP_LE && cond2->op == OP_LT) ||
                (cond1->op == OP_GT && cond2->op == OP_GE) ||
                (cond1->op == OP_GE && cond2->op == OP_GT)) {
                CHECK_EQ(cond1->union_cond(*cond2), OLAP_SUCCESS);
                break;
            }
        }
    }
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
    auto it = _cond_cols.find(index);
    if (it == _cond_cols.end()) {
        cond_col = new CondColumn(*_schema, index);
        _cond_cols[index] = cond_col;
    } else {
        cond_col = it->second;
    }

    return cond_col->add_cond(tcond, column);
}

bool Conditions::merge_del_condition(const CondColumns& cond_cols) {
    if (cond_cols.size() > 1) {
        // Only support to merge on single column
        return false;
    }
    for (const auto& cond_col : cond_cols) {
        int32_t index = cond_col.first;
        auto it = _cond_cols.find(index);
        if (it == _cond_cols.end()) {
            if (!_cond_cols.empty()) {
                // Only support to merge on the same column
                return false;
            }
            CondColumn* new_cond_col = new CondColumn(*_schema, index);
            new_cond_col->_conds = cond_col.second->conds();
            _cond_cols[index] = new_cond_col;
        } else {
            it->second->merge_cond(*cond_col.second);
        }
    }
    return true;
}

bool Conditions::delete_conditions_eval(const RowCursor& row) const {
    if (_cond_cols.empty()) {
        return false;
    }

    for (const auto& cond_col : _cond_cols) {
        if (_cond_column_is_key_or_duplicate(cond_col.second) && !cond_col.second->eval(row)) {
            return false;
        }
    }

    VLOG_NOTICE << "Row meets the delete conditions. "
            << "condition_count=" << _cond_cols.size() << ", row=" << row.to_string();
    return true;
}

bool Conditions::rowset_pruning_filter(const std::vector<KeyRange>& zone_maps) const {
    // ZoneMap will store min/max of rowset.
    // The function is to filter rowset using ZoneMaps
    // and query predicates.
    for (const auto& cond_col : _cond_cols) {
        if (_cond_column_is_key_or_duplicate(cond_col.second)) {
            if (cond_col.first < zone_maps.size() &&
                !cond_col.second->eval(zone_maps.at(cond_col.first))) {
                return true;
            }
        }
    }
    return false;
}

int Conditions::delete_pruning_filter(const std::vector<KeyRange>& zone_maps) const {
    if (_cond_cols.empty()) {
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
    for (auto& cond_col : _cond_cols) {
        /*
         * this is base on the assumption that the delete condition
         * is only about key field, not about value field except the storage model is duplicate.
        */
        if (_cond_column_is_key_or_duplicate(cond_col.second) && cond_col.first > zone_maps.size()) {
            LOG(WARNING) << "where condition not equal column statistics size. "
                         << "cond_id=" << cond_col.first << ", zone_map_size=" << zone_maps.size();
            del_partial_satisfied = true;
            continue;
        }

        int del_ret = cond_col.second->del_eval(zone_maps.at(cond_col.first));
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
    auto iter = _cond_cols.find(cid);
    if (iter != _cond_cols.end()) {
        return iter->second;
    }
    return nullptr;
}

} // namespace doris
