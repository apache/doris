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

#include "olap/delete_handler.h"

#include <errno.h>
#include <json2pb/pb_to_json.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <boost/regex.hpp>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include "gen_cpp/olap_file.pb.h"
#include "olap/olap_common.h"
#include "olap/olap_cond.h"
#include "olap/utils.h"

using apache::thrift::ThriftDebugString;
using std::numeric_limits;
using std::vector;
using std::string;
using std::stringstream;

using boost::regex;
using boost::regex_error;
using boost::regex_match;
using boost::smatch;

using google::protobuf::RepeatedPtrField;

namespace doris {

OLAPStatus DeleteConditionHandler::generate_delete_predicate(
        const TabletSchema& schema, const std::vector<TCondition>& conditions,
        DeletePredicatePB* del_pred) {
    if (conditions.empty()) {
        LOG(WARNING) << "invalid parameters for store_cond."
                     << " condition_size=" << conditions.size();
        return OLAP_ERR_DELETE_INVALID_PARAMETERS;
    }

    // 检查删除条件是否符合要求
    for (const TCondition& condition : conditions) {
        if (check_condition_valid(schema, condition) != OLAP_SUCCESS) {
            LOG(WARNING) << "invalid condition. condition=" << ThriftDebugString(condition);
            return OLAP_ERR_DELETE_INVALID_CONDITION;
        }
    }

    // 存储删除条件
    for (const TCondition& condition : conditions) {
        if (condition.condition_values.size() > 1) {
            InPredicatePB* in_pred = del_pred->add_in_predicates();
            in_pred->set_column_name(condition.column_name);
            bool is_not_in = condition.condition_op == "!*=";
            in_pred->set_is_not_in(is_not_in);
            for (const auto& condition_value : condition.condition_values) {
                in_pred->add_values(condition_value);
            }

            LOG(INFO) << "store one sub-delete condition. condition name=" << in_pred->column_name()
                      << "condition size=" << in_pred->values().size();
        } else {
            string condition_str = construct_sub_predicates(condition);
            del_pred->add_sub_predicates(condition_str);
            LOG(INFO) << "store one sub-delete condition. condition=" << condition_str;
        }
    }
    del_pred->set_version(-1);

    return OLAP_SUCCESS;
}

std::string DeleteConditionHandler::construct_sub_predicates(const TCondition& condition) {
    string op = condition.condition_op;
    if (op == "<") {
        op += "<";
    } else if (op == ">") {
        op += ">";
    }
    string condition_str = "";
    if ("IS" == op) {
        condition_str = condition.column_name + " " + op + " " + condition.condition_values[0];
    } else {
        if (op == "*=") {
            op = "=";
        } else if (op == "!*=") {
            op = "!=";
        }
        condition_str = condition.column_name + op + condition.condition_values[0];
    }
    return condition_str;
}

bool DeleteConditionHandler::is_condition_value_valid(const TabletColumn& column,
                                                      const TCondition& cond,
                                                      const string& value_str) {
    bool valid_condition = false;
    FieldType field_type = column.type();
    if ("IS" == cond.condition_op && ("NULL" == value_str || "NOT NULL" == value_str)) {
        valid_condition = true;
    } else if (field_type == OLAP_FIELD_TYPE_TINYINT) {
        valid_condition = valid_signed_number<int8_t>(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_SMALLINT) {
        valid_condition = valid_signed_number<int16_t>(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_INT) {
        valid_condition = valid_signed_number<int32_t>(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_BIGINT) {
        valid_condition = valid_signed_number<int64_t>(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_LARGEINT) {
        valid_condition = valid_signed_number<int128_t>(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_UNSIGNED_TINYINT) {
        valid_condition = valid_unsigned_number<uint8_t>(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_UNSIGNED_SMALLINT) {
        valid_condition = valid_unsigned_number<uint16_t>(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_UNSIGNED_INT) {
        valid_condition = valid_unsigned_number<uint32_t>(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_UNSIGNED_BIGINT) {
        valid_condition = valid_unsigned_number<uint64_t>(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_DECIMAL) {
        valid_condition = valid_decimal(value_str, column.precision(), column.frac());
    } else if (field_type == OLAP_FIELD_TYPE_CHAR || field_type == OLAP_FIELD_TYPE_VARCHAR) {
        if (value_str.size() <= column.length()) {
            valid_condition = true;
        }
    } else if (field_type == OLAP_FIELD_TYPE_DATE || field_type == OLAP_FIELD_TYPE_DATETIME) {
        valid_condition = valid_datetime(value_str);
    } else if (field_type == OLAP_FIELD_TYPE_BOOL) {
        valid_condition = valid_bool(value_str);
    } else {
        OLAP_LOG_WARNING("unknown field type. [type=%d]", field_type);
    }
    return valid_condition;
}

OLAPStatus DeleteConditionHandler::check_condition_valid(const TabletSchema& schema,
                                                         const TCondition& cond) {
    // 检查指定列名的列是否存在
    int32_t field_index = schema.field_index(cond.column_name);
    if (field_index < 0) {
        OLAP_LOG_WARNING("field is not existent. [field_index=%d]", field_index);
        return OLAP_ERR_DELETE_INVALID_CONDITION;
    }

    // 检查指定的列是不是key，是不是float或double类型
    const TabletColumn& column = schema.column(field_index);

    if ((!column.is_key() && schema.keys_type() != KeysType::DUP_KEYS) ||
        column.type() == OLAP_FIELD_TYPE_DOUBLE || column.type() == OLAP_FIELD_TYPE_FLOAT) {
        LOG(WARNING) << "field is not key column, or storage model is not duplicate, or data type "
                        "is float or double.";
        return OLAP_ERR_DELETE_INVALID_CONDITION;
    }

    // 检查删除条件中指定的过滤值是否符合每个类型自身的要求
    // 1. 对于整数类型(int8,int16,in32,int64,uint8,uint16,uint32,uint64)，检查是否溢出
    // 2. 对于decimal类型，检查是否超过建表时指定的精度和标度
    // 3. 对于date和datetime类型，检查指定的过滤值是否符合日期格式以及是否指定错误的值
    // 4. 对于string和varchar类型，检查指定的过滤值是否超过建表时指定的长度
    if ("*=" != cond.condition_op && "!*=" != cond.condition_op &&
        cond.condition_values.size() != 1) {
        OLAP_LOG_WARNING("invalid condition value size. [size=%ld]", cond.condition_values.size());
        return OLAP_ERR_DELETE_INVALID_CONDITION;
    }

    for (int i = 0; i < cond.condition_values.size(); i++) {
        const string& value_str = cond.condition_values[i];
        if (!is_condition_value_valid(column, cond, value_str)) {
            LOG(WARNING) << "invalid condition value. [value=" << value_str << "]";
            return OLAP_ERR_DELETE_INVALID_CONDITION;
        }
    }

    return OLAP_SUCCESS;
}

bool DeleteHandler::_parse_condition(const std::string& condition_str, TCondition* condition) {
    bool matched = true;
    smatch what;

    try {
        // Condition string format, the format is (column_name)(op)(value)
        // eg:  condition_str="c1 = 1597751948193618247  and length(source)<1;\n;\n"
        //  group1:  (\w+) matches "c1"
        //  group2:  ((?:=)|(?:!=)|(?:>>)|(?:<<)|(?:>=)|(?:<=)|(?:\*=)|(?:IS)) matches  "="
        //  group3:  ((?:[\s\S]+)?) matches "1597751948193618247  and length(source)<1;\n;\n"
        const char* const CONDITION_STR_PATTERN =
                R"((\w+)\s*((?:=)|(?:!=)|(?:>>)|(?:<<)|(?:>=)|(?:<=)|(?:\*=)|(?:IS))\s*((?:[\s\S]+)?))";
        regex ex(CONDITION_STR_PATTERN);
        if (regex_match(condition_str, what, ex)) {
            if (condition_str.size() != what[0].str().size()) {
                matched = false;
            }
        } else {
            matched = false;
        }
    } catch (regex_error& e) {
        VLOG(3) << "fail to parse expr. [expr=" << condition_str << "; error=" << e.what() << "]";
        matched = false;
    }

    if (!matched) {
        return false;
    }
    condition->column_name = what[1].str();
    condition->condition_op = what[2].str();
    condition->condition_values.push_back(what[3].str());

    return true;
}

OLAPStatus DeleteHandler::init(const TabletSchema& schema,
                               const DelPredicateArray& delete_conditions, int32_t version) {
    DCHECK(!_is_inited) << "reinitialize delete handler.";
    DCHECK(version >= 0) << "invalid parameters. version=" << version;

    DelPredicateArray::const_iterator it = delete_conditions.begin();
    for (; it != delete_conditions.end(); ++it) {
        // 跳过版本号大于version的过滤条件
        if (it->version() > version) {
            continue;
        }

        DeleteConditions temp;
        temp.filter_version = it->version();
        temp.del_cond = new (std::nothrow) Conditions();

        if (temp.del_cond == nullptr) {
            LOG(FATAL) << "fail to malloc Conditions. size=" << sizeof(Conditions);
            return OLAP_ERR_MALLOC_ERROR;
        }

        temp.del_cond->set_tablet_schema(&schema);
        for (int i = 0; i != it->sub_predicates_size(); ++i) {
            TCondition condition;
            if (!_parse_condition(it->sub_predicates(i), &condition)) {
                OLAP_LOG_WARNING("fail to parse condition. [condition=%s]",
                                 it->sub_predicates(i).c_str());
                return OLAP_ERR_DELETE_INVALID_PARAMETERS;
            }

            OLAPStatus res = temp.del_cond->append_condition(condition);
            if (OLAP_SUCCESS != res) {
                OLAP_LOG_WARNING("fail to append condition.[res=%d]", res);
                return res;
            }
        }

        for (int i = 0; i != it->in_predicates_size(); ++i) {
            TCondition condition;
            const InPredicatePB& in_predicate = it->in_predicates(i);
            condition.__set_column_name(in_predicate.column_name());
            if (in_predicate.is_not_in()) {
                condition.__set_condition_op("!*=");
            } else {
                condition.__set_condition_op("*=");
            }
            for (const auto& value : in_predicate.values()) {
                condition.condition_values.push_back(value);
            }
            OLAPStatus res = temp.del_cond->append_condition(condition);
            if (OLAP_SUCCESS != res) {
                OLAP_LOG_WARNING("fail to append condition.[res=%d]", res);
                return res;
            }
        }

        _del_conds.push_back(temp);
    }

    _is_inited = true;

    return OLAP_SUCCESS;
}

bool DeleteHandler::is_filter_data(const int32_t data_version, const RowCursor& row) const {
    if (_del_conds.empty()) {
        return false;
    }

    // 根据语义，存储在_del_conds的删除条件应该是OR关系
    // 因此，只要数据符合其中一条过滤条件，则返回true
    std::vector<DeleteConditions>::const_iterator it = _del_conds.begin();

    for (; it != _del_conds.end(); ++it) {
        if (data_version <= it->filter_version && it->del_cond->delete_conditions_eval(row)) {
            return true;
        }
    }

    return false;
}

std::vector<int32_t> DeleteHandler::get_conds_version() {
    std::vector<int32_t> conds_version;
    std::vector<DeleteConditions>::const_iterator cond_iter = _del_conds.begin();

    for (; cond_iter != _del_conds.end(); ++cond_iter) {
        conds_version.push_back(cond_iter->filter_version);
    }

    return conds_version;
}

void DeleteHandler::finalize() {
    if (!_is_inited) {
        return;
    }

    std::vector<DeleteConditions>::iterator it = _del_conds.begin();

    for (; it != _del_conds.end(); ++it) {
        it->del_cond->finalize();
        delete it->del_cond;
    }

    _del_conds.clear();
    _is_inited = false;
}

void DeleteHandler::get_delete_conditions_after_version(
        int32_t version, std::vector<const Conditions*>* delete_conditions) const {
    for (auto& del_cond : _del_conds) {
        if (del_cond.filter_version > version) {
            delete_conditions->emplace_back(del_cond.del_cond);
        }
    }
}

} // namespace doris
