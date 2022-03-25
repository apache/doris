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

#include <limits>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

#include "gen_cpp/olap_file.pb.h"
#include "olap/olap_common.h"
#include "olap/olap_cond.h"
#include "olap/reader.h"
#include "olap/utils.h"

using apache::thrift::ThriftDebugString;
using std::numeric_limits;
using std::vector;
using std::string;
using std::stringstream;

using std::regex;
using std::regex_error;
using std::regex_match;
using std::smatch;

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

    // Check whether the delete condition meets the requirements
    for (const TCondition& condition : conditions) {
        if (check_condition_valid(schema, condition) != OLAP_SUCCESS) {
            LOG(WARNING) << "invalid condition. condition=" << ThriftDebugString(condition);
            return OLAP_ERR_DELETE_INVALID_CONDITION;
        }
    }

    // Store delete condition
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
                                                      const std::string& condition_op,
                                                      const string& value_str) {
    if ("IS" == condition_op && ("NULL" == value_str || "NOT NULL" == value_str)) {
        return true;
    }

    FieldType field_type = column.type();
    switch (field_type) {
    case OLAP_FIELD_TYPE_TINYINT:
        return valid_signed_number<int8_t>(value_str);
    case OLAP_FIELD_TYPE_SMALLINT:
        return valid_signed_number<int16_t>(value_str);
    case OLAP_FIELD_TYPE_INT:
        return valid_signed_number<int32_t>(value_str);
    case OLAP_FIELD_TYPE_BIGINT:
        return valid_signed_number<int64_t>(value_str);
    case OLAP_FIELD_TYPE_LARGEINT:
        return valid_signed_number<int128_t>(value_str);
    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
        return valid_unsigned_number<uint8_t>(value_str);
    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
        return valid_unsigned_number<uint16_t>(value_str);
    case OLAP_FIELD_TYPE_UNSIGNED_INT:
        return valid_unsigned_number<uint32_t>(value_str);
    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        return valid_unsigned_number<uint64_t>(value_str);
    case OLAP_FIELD_TYPE_DECIMAL:
        return valid_decimal(value_str, column.precision(), column.frac());
    case OLAP_FIELD_TYPE_CHAR:
    case OLAP_FIELD_TYPE_VARCHAR:
        return value_str.size() <= column.length();
    case OLAP_FIELD_TYPE_STRING:
        return value_str.size() <= config::string_type_length_soft_limit_bytes;
    case OLAP_FIELD_TYPE_DATE:
    case OLAP_FIELD_TYPE_DATETIME:
        return valid_datetime(value_str);
    case OLAP_FIELD_TYPE_BOOL:
        return valid_bool(value_str);
    default:
        OLAP_LOG_WARNING("unknown field type. [type=%d]", field_type);
    }
    return false;
}

OLAPStatus DeleteConditionHandler::check_condition_valid(const TabletSchema& schema,
                                                         const TCondition& cond) {
    // Check whether the column exists
    int32_t field_index = schema.field_index(cond.column_name);
    if (field_index < 0) {
        OLAP_LOG_WARNING("field is not existent. [field_index=%d]", field_index);
        return OLAP_ERR_DELETE_INVALID_CONDITION;
    }

    // Delete condition should only applied on key columns or duplicate key table, and
    // the condition column type should not be float or double.
    const TabletColumn& column = schema.column(field_index);

    if ((!column.is_key() && schema.keys_type() != KeysType::DUP_KEYS) ||
        column.type() == OLAP_FIELD_TYPE_DOUBLE || column.type() == OLAP_FIELD_TYPE_FLOAT) {
        LOG(WARNING) << "field is not key column, or storage model is not duplicate, or data type "
                        "is float or double.";
        return OLAP_ERR_DELETE_INVALID_CONDITION;
    }

    // Check operator and operands size are matched.
    if ("*=" != cond.condition_op && "!*=" != cond.condition_op &&
        cond.condition_values.size() != 1) {
        OLAP_LOG_WARNING("invalid condition value size. [size=%ld]", cond.condition_values.size());
        return OLAP_ERR_DELETE_INVALID_CONDITION;
    }

    // Check each operand is valid
    for (const auto& condition_value : cond.condition_values) {
        if (!is_condition_value_valid(column, cond.condition_op, condition_value)) {
            LOG(WARNING) << "invalid condition value. [value=" << condition_value << "]";
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
        VLOG_NOTICE << "fail to parse expr. [expr=" << condition_str << "; error=" << e.what()
                    << "]";
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
                               const DelPredicateArray& delete_conditions, int64_t version,
                               const TabletReader* reader) {
    DCHECK(!_is_inited) << "reinitialize delete handler.";
    DCHECK(version >= 0) << "invalid parameters. version=" << version;

    for (const auto& delete_condition : delete_conditions) {
        // Skip the delete condition with large version
        if (delete_condition.version() > version) {
            continue;
        }

        DeleteConditions temp;
        temp.filter_version = delete_condition.version();
        temp.del_cond = new (std::nothrow) Conditions();

        if (temp.del_cond == nullptr) {
            LOG(FATAL) << "fail to malloc Conditions. size=" << sizeof(Conditions);
            return OLAP_ERR_MALLOC_ERROR;
        }

        temp.del_cond->set_tablet_schema(&schema);
        for (const auto& sub_predicate : delete_condition.sub_predicates()) {
            TCondition condition;
            if (!_parse_condition(sub_predicate, &condition)) {
                OLAP_LOG_WARNING("fail to parse condition. [condition=%s]", sub_predicate.c_str());
                return OLAP_ERR_DELETE_INVALID_PARAMETERS;
            }

            OLAPStatus res = temp.del_cond->append_condition(condition);
            if (OLAP_SUCCESS != res) {
                OLAP_LOG_WARNING("fail to append condition.[res=%d]", res);
                return res;
            }

            if (reader != nullptr) {
                auto predicate = reader->_parse_to_predicate(condition, true);
                if (predicate != nullptr) {
                    temp.column_predicate_vec.push_back(predicate);
                }
            }
        }

        for (const auto& in_predicate : delete_condition.in_predicates()) {
            TCondition condition;
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

            if (reader != nullptr) {
                temp.column_predicate_vec.push_back(reader->_parse_to_predicate(condition, true));
            }
        }

        _del_conds.emplace_back(std::move(temp));
    }

    _is_inited = true;

    return OLAP_SUCCESS;
}

bool DeleteHandler::is_filter_data(const int64_t data_version, const RowCursor& row) const {
    // According to semantics, the delete condition stored in _del_conds should be an OR relationship,
    // so as long as the data matches one of the _del_conds, it will return true.
    for (const auto& del_cond : _del_conds) {
        if (data_version <= del_cond.filter_version &&
            del_cond.del_cond->delete_conditions_eval(row)) {
            return true;
        }
    }

    return false;
}

std::vector<int64_t> DeleteHandler::get_conds_version() {
    std::vector<int64_t> conds_version;
    for (const auto& cond : _del_conds) {
        conds_version.push_back(cond.filter_version);
    }

    return conds_version;
}

void DeleteHandler::finalize() {
    if (!_is_inited) {
        return;
    }

    for (auto& cond : _del_conds) {
        cond.del_cond->finalize();
        delete cond.del_cond;

        for (auto pred : cond.column_predicate_vec) {
            delete pred;
        }
    }

    _del_conds.clear();
    _is_inited = false;
}

void DeleteHandler::get_delete_conditions_after_version(
        int64_t version, std::vector<const Conditions*>* delete_conditions,
        AndBlockColumnPredicate* and_block_column_predicate_ptr) const {
    for (auto& del_cond : _del_conds) {
        if (del_cond.filter_version > version) {
            delete_conditions->emplace_back(del_cond.del_cond);

            // now, only query support delete column predicate operator
            if (!del_cond.column_predicate_vec.empty()) {
                if (del_cond.column_predicate_vec.size() == 1) {
                    auto single_column_block_predicate =
                            new SingleColumnBlockPredicate(del_cond.column_predicate_vec[0]);
                    and_block_column_predicate_ptr->add_column_predicate(
                            single_column_block_predicate);
                } else {
                    auto or_column_predicate = new OrBlockColumnPredicate();

                    // build or_column_predicate
                    std::for_each(del_cond.column_predicate_vec.cbegin(),
                                  del_cond.column_predicate_vec.cend(),
                                  [&or_column_predicate](const ColumnPredicate* predicate) {
                                      or_column_predicate->add_column_predicate(
                                              new SingleColumnBlockPredicate(predicate));
                                  });
                    and_block_column_predicate_ptr->add_column_predicate(or_column_predicate);
                }
            }
        }
    }
}

} // namespace doris
