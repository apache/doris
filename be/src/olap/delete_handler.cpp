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

#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <limits>
#include <regex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "olap/block_column_predicate.h"
#include "olap/column_predicate.h"
#include "olap/olap_common.h"
#include "olap/predicate_creator.h"
#include "olap/utils.h"

using apache::thrift::ThriftDebugString;
using std::vector;
using std::string;
using std::stringstream;

using std::regex;
using std::regex_error;
using std::regex_match;
using std::smatch;

namespace doris {
using namespace ErrorCode;

Status DeleteHandler::generate_delete_predicate(const TabletSchema& schema,
                                                const std::vector<TCondition>& conditions,
                                                DeletePredicatePB* del_pred) {
    if (conditions.empty()) {
        return Status::Error<DELETE_INVALID_PARAMETERS>(
                "invalid parameters for store_cond. condition_size={}", conditions.size());
    }

    // Check whether the delete condition meets the requirements
    for (const TCondition& condition : conditions) {
        if (!check_condition_valid(schema, condition).ok()) {
            // Error will print log, no need to do it manually.
            return Status::Error<DELETE_INVALID_CONDITION>("invalid condition. condition={}",
                                                           ThriftDebugString(condition));
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
    string condition_str;
    if ("IS" == op) {
        condition_str = condition.column_name + " " + op + " " + condition.condition_values[0];
    } else {
        if (op == "*=") {
            op = "=";
        } else if (op == "!*=") {
            op = "!=";
        }
        condition_str = condition.column_name + op + "'" + condition.condition_values[0] + "'";
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
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        return valid_signed_number<int8_t>(value_str);
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        return valid_signed_number<int16_t>(value_str);
    case FieldType::OLAP_FIELD_TYPE_INT:
        return valid_signed_number<int32_t>(value_str);
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        return valid_signed_number<int64_t>(value_str);
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        return valid_signed_number<int128_t>(value_str);
    case FieldType::OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
        return valid_unsigned_number<uint8_t>(value_str);
    case FieldType::OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
        return valid_unsigned_number<uint16_t>(value_str);
    case FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT:
        return valid_unsigned_number<uint32_t>(value_str);
    case FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        return valid_unsigned_number<uint64_t>(value_str);
    case FieldType::OLAP_FIELD_TYPE_DECIMAL:
        return valid_decimal(value_str, column.precision(), column.frac());
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
        return valid_decimal(value_str, column.precision(), column.frac());
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
        return valid_decimal(value_str, column.precision(), column.frac());
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
        return valid_decimal(value_str, column.precision(), column.frac());
    case FieldType::OLAP_FIELD_TYPE_CHAR:
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
        return value_str.size() <= column.length();
    case FieldType::OLAP_FIELD_TYPE_STRING:
        return value_str.size() <= config::string_type_length_soft_limit_bytes;
    case FieldType::OLAP_FIELD_TYPE_DATE:
    case FieldType::OLAP_FIELD_TYPE_DATETIME:
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
        return valid_datetime(value_str, column.frac());
    case FieldType::OLAP_FIELD_TYPE_BOOL:
        return valid_bool(value_str);
    default:
        LOG(WARNING) << "unknown field type. [type=" << int(field_type) << "]";
    }
    return false;
}

Status DeleteHandler::check_condition_valid(const TabletSchema& schema, const TCondition& cond) {
    // Check whether the column exists
    int32_t field_index = schema.field_index(cond.column_name);
    if (field_index < 0) {
        return Status::Error<DELETE_INVALID_CONDITION>("field is not existent. [field_index={}]",
                                                       field_index);
    }

    // Delete condition should only applied on key columns or duplicate key table, and
    // the condition column type should not be float or double.
    const TabletColumn& column = schema.column(field_index);

    if (column.type() == FieldType::OLAP_FIELD_TYPE_DOUBLE ||
        column.type() == FieldType::OLAP_FIELD_TYPE_FLOAT) {
        return Status::Error<DELETE_INVALID_CONDITION>("data type is float or double.");
    }

    // Check operator and operands size are matched.
    if ("*=" != cond.condition_op && "!*=" != cond.condition_op &&
        cond.condition_values.size() != 1) {
        return Status::Error<DELETE_INVALID_CONDITION>("invalid condition value size. [size={}]",
                                                       cond.condition_values.size());
    }

    // Check each operand is valid
    for (const auto& condition_value : cond.condition_values) {
        if (!is_condition_value_valid(column, cond.condition_op, condition_value)) {
            return Status::Error<DELETE_INVALID_CONDITION>("invalid condition value. [value={}]",
                                                           condition_value);
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
                R"(([\w$#%]+)\s*((?:=)|(?:!=)|(?:>>)|(?:<<)|(?:>=)|(?:<=)|(?:\*=)|(?:IS))\s*('((?:[\s\S]+)?)'|(?:[\s\S]+)?))";
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
    if (what[4].matched) { // match string with single quotes, eg. a = 'b'
        condition->condition_values.push_back(what[4].str());
    } else { // match string without quote, compat with old conditions, eg. a = b
        condition->condition_values.push_back(what[3].str());
    }

    return true;
}

Status DeleteHandler::init(TabletSchemaSPtr tablet_schema,
                           const std::vector<RowsetMetaSharedPtr>& delete_preds, int64_t version) {
    DCHECK(!_is_inited) << "reinitialize delete handler.";
    DCHECK(version >= 0) << "invalid parameters. version=" << version;
    _predicate_arena.reset(new vectorized::Arena());

    for (const auto& delete_pred : delete_preds) {
        // Skip the delete condition with large version
        if (delete_pred->version().first > version) {
            continue;
        }
        // Need the tablet schema at the delete condition to parse the accurate column
        const auto& delete_pred_related_schema = delete_pred->tablet_schema();
        auto& delete_condition = delete_pred->delete_predicate();
        DeleteConditions temp;
        temp.filter_version = delete_pred->version().first;
        for (const auto& sub_predicate : delete_condition.sub_predicates()) {
            TCondition condition;
            if (!_parse_condition(sub_predicate, &condition)) {
                return Status::Error<DELETE_INVALID_PARAMETERS>(
                        "fail to parse condition. condition={}", sub_predicate);
            }
            int32_t col_unique_id =
                    delete_pred_related_schema->column(condition.column_name).unique_id();
            const auto& column = tablet_schema->column_by_uid(col_unique_id);
            uint32_t index = tablet_schema->field_index(col_unique_id);
            auto predicate =
                    parse_to_predicate(column, index, condition, _predicate_arena.get(), true);
            if (predicate != nullptr) {
                temp.column_predicate_vec.push_back(predicate);
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
            int32_t col_unique_id =
                    delete_pred_related_schema->column(condition.column_name).unique_id();
            const auto& column = tablet_schema->column_by_uid(col_unique_id);
            uint32_t index = tablet_schema->field_index(col_unique_id);
            temp.column_predicate_vec.push_back(
                    parse_to_predicate(column, index, condition, _predicate_arena.get(), true));
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
        std::unordered_map<int32_t, std::vector<const ColumnPredicate*>>*
                del_predicates_for_zone_map) const {
    for (auto& del_cond : _del_conds) {
        if (del_cond.filter_version > version) {
            // now, only query support delete column predicate operator
            if (!del_cond.column_predicate_vec.empty()) {
                if (del_cond.column_predicate_vec.size() == 1) {
                    auto single_column_block_predicate =
                            new SingleColumnBlockPredicate(del_cond.column_predicate_vec[0]);
                    and_block_column_predicate_ptr->add_column_predicate(
                            single_column_block_predicate);
                    if (del_predicates_for_zone_map->count(
                                del_cond.column_predicate_vec[0]->column_id()) < 1) {
                        del_predicates_for_zone_map->insert(
                                {del_cond.column_predicate_vec[0]->column_id(),
                                 std::vector<const ColumnPredicate*> {}});
                    }
                    (*del_predicates_for_zone_map)[del_cond.column_predicate_vec[0]->column_id()]
                            .push_back(del_cond.column_predicate_vec[0]);
                } else {
                    auto or_column_predicate = new OrBlockColumnPredicate();

                    // build or_column_predicate
                    // when delete from where a = 1 and b = 2, we can not use del_predicates_for_zone_map to filter zone page,
                    // so here do not put predicate to del_predicates_for_zone_map,
                    // refer #17145 for more details.
                    // // TODO: need refactor design and code to use more version delete and more column delete to filter zone page.
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
