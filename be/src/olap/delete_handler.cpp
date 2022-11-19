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
#include "olap/predicate_creator.h"
#include "olap/tablet.h"
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

Status DeleteHandler::generate_delete_predicate(const TabletSchema& schema,
                                                const std::vector<TCondition>& conditions,
                                                DeletePredicatePB* del_pred) {
    if (conditions.empty()) {
        LOG(WARNING) << "invalid parameters for store_cond."
                     << " condition_size=" << conditions.size();
        return Status::OLAPInternalError(OLAP_ERR_DELETE_INVALID_PARAMETERS);
    }

    // Check whether the delete condition meets the requirements
    for (const TCondition& condition : conditions) {
        if (check_condition_valid(schema, condition) != Status::OK()) {
            LOG(WARNING) << "invalid condition. condition=" << ThriftDebugString(condition);
            return Status::OLAPInternalError(OLAP_ERR_DELETE_INVALID_CONDITION);
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

    return Status::OK();
}

std::string DeleteHandler::construct_sub_predicates(const TCondition& condition) {
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

bool DeleteHandler::is_condition_value_valid(const TabletColumn& column,
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
    case OLAP_FIELD_TYPE_DECIMAL32:
        return valid_decimal(value_str, column.precision(), column.frac());
    case OLAP_FIELD_TYPE_DECIMAL64:
        return valid_decimal(value_str, column.precision(), column.frac());
    case OLAP_FIELD_TYPE_DECIMAL128I:
        return valid_decimal(value_str, column.precision(), column.frac());
    case OLAP_FIELD_TYPE_CHAR:
    case OLAP_FIELD_TYPE_VARCHAR:
        return value_str.size() <= column.length();
    case OLAP_FIELD_TYPE_STRING:
        return value_str.size() <= config::string_type_length_soft_limit_bytes;
    case OLAP_FIELD_TYPE_DATE:
    case OLAP_FIELD_TYPE_DATETIME:
    case OLAP_FIELD_TYPE_DATEV2:
    case OLAP_FIELD_TYPE_DATETIMEV2:
        return valid_datetime(value_str, column.frac());
    case OLAP_FIELD_TYPE_BOOL:
        return valid_bool(value_str);
    default:
        LOG(WARNING) << "unknown field type. [type=" << field_type << "]";
    }
    return false;
}

Status DeleteHandler::check_condition_valid(const TabletSchema& schema, const TCondition& cond) {
    // Check whether the column exists
    int32_t field_index = schema.field_index(cond.column_name);
    if (field_index < 0) {
        LOG(WARNING) << "field is not existent. [field_index=" << field_index << "]";
        return Status::OLAPInternalError(OLAP_ERR_DELETE_INVALID_CONDITION);
    }

    // Delete condition should only applied on key columns or duplicate key table, and
    // the condition column type should not be float or double.
    const TabletColumn& column = schema.column(field_index);

    if ((!column.is_key() && schema.keys_type() != KeysType::DUP_KEYS) ||
        column.type() == OLAP_FIELD_TYPE_DOUBLE || column.type() == OLAP_FIELD_TYPE_FLOAT) {
        LOG(WARNING) << "field is not key column, or storage model is not duplicate, or data type "
                        "is float or double.";
        return Status::OLAPInternalError(OLAP_ERR_DELETE_INVALID_CONDITION);
    }

    // Check operator and operands size are matched.
    if ("*=" != cond.condition_op && "!*=" != cond.condition_op &&
        cond.condition_values.size() != 1) {
        LOG(WARNING) << "invalid condition value size. [size=" << cond.condition_values.size()
                     << "]";
        return Status::OLAPInternalError(OLAP_ERR_DELETE_INVALID_CONDITION);
    }

    // Check each operand is valid
    for (const auto& condition_value : cond.condition_values) {
        if (!is_condition_value_valid(column, cond.condition_op, condition_value)) {
            LOG(WARNING) << "invalid condition value. [value=" << condition_value << "]";
            return Status::OLAPInternalError(OLAP_ERR_DELETE_INVALID_CONDITION);
        }
    }

    return Status::OK();
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

Status DeleteHandler::init(TabletSchemaSPtr tablet_schema,
                           const std::vector<RowsetMetaSharedPtr>& delete_preds, int64_t version) {
    DCHECK(!_is_inited) << "reinitialize delete handler.";
    DCHECK(version >= 0) << "invalid parameters. version=" << version;
    _predicate_mem_pool.reset(new MemPool());

    for (const auto& delete_pred : delete_preds) {
        // Skip the delete condition with large version
        if (delete_pred->version().first > version) {
            continue;
        }
        // Need the tablet schema at the delete condition to parse the accurate column unique id
        TabletSchemaSPtr delete_pred_related_schema = delete_pred->tablet_schema();
        auto& delete_condition = delete_pred->delete_predicate();
        DeleteConditions temp;
        temp.filter_version = delete_pred->version().first;
        for (const auto& sub_predicate : delete_condition.sub_predicates()) {
            TCondition condition;
            if (!_parse_condition(sub_predicate, &condition)) {
                LOG(WARNING) << "fail to parse condition. [condition=" << sub_predicate << "]";
                return Status::OLAPInternalError(OLAP_ERR_DELETE_INVALID_PARAMETERS);
            }
            condition.__set_column_unique_id(
                    delete_pred_related_schema->column(condition.column_name).unique_id());
            auto predicate =
                    parse_to_predicate(tablet_schema, condition, _predicate_mem_pool.get(), true);
            if (predicate != nullptr) {
                temp.column_predicate_vec.push_back(predicate);
            }
        }

        for (const auto& in_predicate : delete_condition.in_predicates()) {
            TCondition condition;
            condition.__set_column_name(in_predicate.column_name());
            condition.__set_column_unique_id(
                    delete_pred_related_schema->column(condition.column_name).unique_id());
            if (in_predicate.is_not_in()) {
                condition.__set_condition_op("!*=");
            } else {
                condition.__set_condition_op("*=");
            }
            for (const auto& value : in_predicate.values()) {
                condition.condition_values.push_back(value);
            }
            temp.column_predicate_vec.push_back(
                    parse_to_predicate(tablet_schema, condition, _predicate_mem_pool.get(), true));
        }

        _del_conds.emplace_back(std::move(temp));
    }

    _is_inited = true;

    return Status::OK();
}

void DeleteHandler::finalize() {
    if (!_is_inited) {
        return;
    }

    for (auto& cond : _del_conds) {
        for (auto pred : cond.column_predicate_vec) {
            delete pred;
        }
    }

    _del_conds.clear();
    _is_inited = false;
}

void DeleteHandler::get_delete_conditions_after_version(
        int64_t version, AndBlockColumnPredicate* and_block_column_predicate_ptr,
        std::unordered_map<int32_t, std::vector<const ColumnPredicate*>>* col_id_to_del_predicates)
        const {
    for (auto& del_cond : _del_conds) {
        if (del_cond.filter_version > version) {
            // now, only query support delete column predicate operator
            if (!del_cond.column_predicate_vec.empty()) {
                if (del_cond.column_predicate_vec.size() == 1) {
                    auto single_column_block_predicate =
                            new SingleColumnBlockPredicate(del_cond.column_predicate_vec[0]);
                    and_block_column_predicate_ptr->add_column_predicate(
                            single_column_block_predicate);
                    if (col_id_to_del_predicates->count(
                                del_cond.column_predicate_vec[0]->column_id()) < 1) {
                        col_id_to_del_predicates->insert(
                                {del_cond.column_predicate_vec[0]->column_id(),
                                 std::vector<const ColumnPredicate*> {}});
                    }
                    (*col_id_to_del_predicates)[del_cond.column_predicate_vec[0]->column_id()]
                            .push_back(del_cond.column_predicate_vec[0]);
                } else {
                    auto or_column_predicate = new OrBlockColumnPredicate();

                    // build or_column_predicate
                    std::for_each(
                            del_cond.column_predicate_vec.cbegin(),
                            del_cond.column_predicate_vec.cend(),
                            [&or_column_predicate,
                             col_id_to_del_predicates](const ColumnPredicate* predicate) {
                                if (col_id_to_del_predicates->count(predicate->column_id()) < 1) {
                                    col_id_to_del_predicates->insert(
                                            {predicate->column_id(),
                                             std::vector<const ColumnPredicate*> {}});
                                }
                                (*col_id_to_del_predicates)[predicate->column_id()].push_back(
                                        predicate);
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
