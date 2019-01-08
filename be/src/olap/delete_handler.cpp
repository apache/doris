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

#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include <boost/regex.hpp>
#include <errno.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "gen_cpp/olap_file.pb.h"
#include "olap/olap_common.h"
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

string DeleteConditionHandler::construct_sub_conditions(const TCondition& condition) {
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
        condition_str = condition.column_name + op + condition.condition_values[0];
    }
    return condition_str;
}

// 删除指定版本号的删除条件；需要注意的是，如果table上没有任何删除条件，或者
// 指定版本号的删除条件不存在，也会返回OLAP_SUCCESS。
OLAPStatus DeleteConditionHandler::delete_cond(DelPredicateArray* delete_conditions,
        const int32_t version,
        bool delete_smaller_version_conditions) {
    if (version < 0) {
        OLAP_LOG_WARNING("invalid parameters for delete_cond. [version=%d]", version);
        return OLAP_ERR_DELETE_INVALID_PARAMETERS;
    }

    if (delete_conditions->size() == 0) {
        return OLAP_SUCCESS;
    }

    int index = 0;

    while (index != delete_conditions->size()) {
        // 1. 如果删除条件的版本号等于形参指定的版本号，则删除该版本的文件；
        // 2. 如果还指定了delete_smaller_version_conditions为true，则同时删除
        //    版本号小于指定版本号的删除条件；否则不删除。
        DeletePredicatePB temp = delete_conditions->Get(index);

        if (temp.version() == version ||
                (temp.version() < version && delete_smaller_version_conditions)) {
            // 将要移除的删除条件记录到log中
            string del_cond_str;
            const RepeatedPtrField<string>& sub_conditions = temp.sub_conditions();

            for (int i = 0; i != sub_conditions.size(); ++i) {
                del_cond_str += sub_conditions.Get(i) + ";";
            }

            LOG(INFO) << "delete one condition. version=" << temp.version()
                      << ", condition=" << del_cond_str;

            // 移除过滤条件
            // 因为pb没有提供直接删除数组特定元素的方法，所以用下面的删除方式；这种方式会改变存在
            // Header文件中的删除条件的顺序。因为我们不关心删除条件的顺序，所以对我们没影响
            delete_conditions->SwapElements(index, delete_conditions->size() - 1);
            delete_conditions->RemoveLast();
        } else {
            ++index;
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus DeleteConditionHandler::log_conds(std::string tablet_full_name,
        const DelPredicateArray& delete_conditions) {
    LOG(INFO) << "display all delete condition. tablet=" << tablet_full_name;

    for (int index = 0; index != delete_conditions.size(); ++index) {
        DeletePredicatePB temp = delete_conditions.Get(index);
        string del_cond_str;
        const RepeatedPtrField<string>& sub_conditions = temp.sub_conditions();

        // 将属于一条删除条件的子条件重新拼接成一条删除条件；子条件之间用分号隔开
        for (int i = 0; i != sub_conditions.size(); ++i) {
            del_cond_str += sub_conditions.Get(i) + ";";
        }

        LOG(INFO) << "condition item: version=" << temp.version()
                  << ", condition=" << del_cond_str;
    }
    return OLAP_SUCCESS;
}

OLAPStatus DeleteConditionHandler::check_condition_valid(
        const RowFields& tablet_schema,
        const TCondition& cond) {
    // 检查指定列名的列是否存在
    int field_index = _get_field_index(tablet_schema, cond.column_name);

    if (field_index < 0) {
        OLAP_LOG_WARNING("field is not existent. [field_index=%d]", field_index);
        return OLAP_ERR_DELETE_INVALID_CONDITION;
    }

    // 检查指定的列是不是key，是不是float或doulbe类型
    FieldInfo field_info = tablet_schema[field_index];

    if (!field_info.is_key
            || field_info.type == OLAP_FIELD_TYPE_DOUBLE
            || field_info.type == OLAP_FIELD_TYPE_FLOAT) {
        OLAP_LOG_WARNING("field is not key column, or its type is float or double.");
        return OLAP_ERR_DELETE_INVALID_CONDITION;
    }

    // 检查删除条件中指定的过滤值是否符合每个类型自身的要求
    // 1. 对于整数类型(int8,int16,in32,int64,uint8,uint16,uint32,uint64)，检查是否溢出
    // 2. 对于decimal类型，检查是否超过建表时指定的精度和标度
    // 3. 对于date和datetime类型，检查指定的过滤值是否符合日期格式以及是否指定错误的值
    // 4. 对于string和varchar类型，检查指定的过滤值是否超过建表时指定的长度
    if (cond.condition_values.size() != 1) {
        OLAP_LOG_WARNING("invalid condition value size. [size=%ld]", cond.condition_values.size());
        return OLAP_ERR_DELETE_INVALID_CONDITION;
    }
    const string& value_str = cond.condition_values[0];

    FieldType field_type = field_info.type;
    bool valid_condition = false;

    if ("IS" == cond.condition_op 
        && ("NULL" == value_str || "NOT NULL" == value_str)) {
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
        valid_condition = valid_decimal(value_str, field_info.precision, field_info.frac);
    } else if (field_type == OLAP_FIELD_TYPE_CHAR || field_type == OLAP_FIELD_TYPE_VARCHAR
               || field_type == OLAP_FIELD_TYPE_HLL) {
        if (value_str.size() <= field_info.length) {
            valid_condition = true;
        }
    } else if (field_type == OLAP_FIELD_TYPE_DATE || field_type == OLAP_FIELD_TYPE_DATETIME) {
        valid_condition = valid_datetime(value_str);
    } else {
        OLAP_LOG_WARNING("unknown field type. [type=%d]", field_type);
    }

    if (valid_condition) {
        return OLAP_SUCCESS;
    } else {
        LOG(WARNING) << "invalid condition value. [value=" << value_str << "]";
        return OLAP_ERR_DELETE_INVALID_CONDITION;
    }
}

OLAPStatus DeleteConditionHandler::_check_version_valid(std::vector<Version>* all_file_versions,
        const int32_t filter_version) {
    // 找到当前最大的delta文件版本号
    int max_delta_version = -1;
    vector<Version>::const_iterator version_iter = all_file_versions->begin();

    for (; version_iter != all_file_versions->end(); ++version_iter) {
        if (version_iter->second > max_delta_version) {
            max_delta_version = version_iter->second;
        }
    }

    if (filter_version == max_delta_version || filter_version == max_delta_version + 1) {
        return OLAP_SUCCESS;
    } else {
        OLAP_LOG_WARNING("invalid delete condition version. [version=%d, max_delta_version=%d]",
                         filter_version, max_delta_version);
        return OLAP_ERR_DELETE_INVALID_VERSION;
    }
}

int DeleteConditionHandler::_check_whether_condition_exist(const DelPredicateArray& delete_conditions, int cond_version) {
    if (delete_conditions.size() == 0) {
        return -1;
    }

    int index = 0;

    while (index != delete_conditions.size()) {
        DeletePredicatePB temp = delete_conditions.Get(index);

        if (temp.version() == cond_version) {
            return index;
        }

        ++index;
    }

    return -1;
}

bool DeleteHandler::_parse_condition(const std::string& condition_str, TCondition* condition) {
    bool matched = true;
    smatch what;

    try {
        // Condition string format
        const char* const CONDITION_STR_PATTERN =
                "(\\w+)\\s*((?:=)|(?:!=)|(?:>>)|(?:<<)|(?:>=)|(?:<=)|(?:\\*=)|(?:IS))\\s*((?:[\\S ]+)?)";
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

OLAPStatus DeleteHandler::init(const RowFields& tablet_schema,
        const DelPredicateArray& delete_conditions, int32_t version) {
    if (_is_inited) {
        OLAP_LOG_WARNING("reintialize delete handler.");
        return OLAP_ERR_INIT_FAILED;

    }

    if (version < 0) {
        OLAP_LOG_WARNING("invalid parameters. [version=%d]", version);
        return OLAP_ERR_DELETE_INVALID_PARAMETERS;
    }

    DelPredicateArray::const_iterator it = delete_conditions.begin();

    for (; it != delete_conditions.end(); ++it) {
        // 跳过版本号大于version的过滤条件
        if (it->version() > version) {
            continue;
        }

        DeleteConditions temp;
        temp.filter_version = it->version();

        temp.del_cond = new(std::nothrow) Conditions();

        if (temp.del_cond == NULL) {
            LOG(FATAL) << "fail to malloc Conditions. size=" << sizeof(Conditions);
            return OLAP_ERR_MALLOC_ERROR;
        }

        temp.del_cond->set_tablet_schema(tablet_schema);

        for (int i = 0; i != it->sub_conditions_size(); ++i) {
            TCondition condition;
            if (!_parse_condition(it->sub_conditions(i), &condition)) {
                OLAP_LOG_WARNING("fail to parse condition. [condition=%s]",
                                 it->sub_conditions(i).c_str());
                return OLAP_ERR_DELETE_INVALID_PARAMETERS;
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

bool DeleteHandler::is_filter_data(const int32_t data_version,
                                   const RowCursor& row) const {
    if (_del_conds.empty()) {
        return false;
    }

    // 根据语义，存储在_del_conds的删除条件应该是OR关系
    // 因此，只要数据符合其中一条过滤条件，则返回true
    vector<DeleteConditions>::const_iterator it = _del_conds.begin();

    for (; it != _del_conds.end(); ++it) {
        if (data_version <= it->filter_version && it->del_cond->delete_conditions_eval(row)) {
            return true;
        }
    }

    return false;
}

vector<int32_t> DeleteHandler::get_conds_version() {
    vector<int32_t> conds_version;
    vector<DeleteConditions>::const_iterator cond_iter = _del_conds.begin();

    for (; cond_iter != _del_conds.end(); ++cond_iter) {
        conds_version.push_back(cond_iter->filter_version);
    }

    return conds_version;
}

void DeleteHandler::finalize() {
    if (!_is_inited) {
        return;
    }

    vector<DeleteConditions>::iterator it = _del_conds.begin();

    for (; it != _del_conds.end(); ++it) {
        it->del_cond->finalize();
        delete it->del_cond;
    }

    _del_conds.clear();
    _is_inited = false;
}

}  // namespace doris

