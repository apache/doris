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

#include <string>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "olap/block_column_predicate.h"
#include "olap/olap_common.h"
#include "olap/predicate_creator.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "util/debug_points.h"
#include "vec/functions/cast/cast_parameters.h"
#include "vec/functions/cast/cast_to_boolean.h"
#include "vec/functions/cast/cast_to_date_or_datetime_impl.hpp"
#include "vec/functions/cast/cast_to_datetimev2_impl.hpp"
#include "vec/functions/cast/cast_to_datev2_impl.hpp"
#include "vec/functions/cast/cast_to_decimal.h"
#include "vec/functions/cast/cast_to_float.h"
#include "vec/functions/cast/cast_to_int.h"
#include "vec/functions/cast/cast_to_ip.h"

using apache::thrift::ThriftDebugString;
using std::vector;
using std::string;

using ::google::protobuf::RepeatedPtrField;

namespace doris {

template <PrimitiveType PType>
Status convert(const vectorized::DataTypePtr& data_type, const std::string& str,
               vectorized::Arena& arena, typename PrimitiveTypeTraits<PType>::CppType& res) {
    if constexpr (PType == TYPE_TINYINT || PType == TYPE_SMALLINT || PType == TYPE_INT ||
                  PType == TYPE_BIGINT || PType == TYPE_LARGEINT) {
        vectorized::CastParameters parameters;
        if (!vectorized::CastToInt::from_string<false>({str.data(), str.size()}, res, parameters)) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "invalid {} string. str={}", type_to_string(data_type->get_primitive_type()),
                    str);
        }
        return Status::OK();
    }
    if constexpr (PType == TYPE_FLOAT || PType == TYPE_DOUBLE) {
        vectorized::CastParameters parameters;
        if (!vectorized::CastToFloat::from_string({str.data(), str.size()}, res, parameters)) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "invalid {} string. str={}", type_to_string(data_type->get_primitive_type()),
                    str);
        }
        return Status::OK();
    }
    if constexpr (PType == TYPE_DATE) {
        vectorized::CastParameters parameters;
        if (!vectorized::CastToDateOrDatetime::from_string<false>({str.data(), str.size()}, res,
                                                                  nullptr, parameters)) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "invalid {} string. str={}", type_to_string(data_type->get_primitive_type()),
                    str);
        }
        return Status::OK();
    }
    if constexpr (PType == TYPE_DATETIME) {
        vectorized::CastParameters parameters;
        if (!vectorized::CastToDateOrDatetime::from_string<true>({str.data(), str.size()}, res,
                                                                 nullptr, parameters)) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "invalid {} string. str={}", type_to_string(data_type->get_primitive_type()),
                    str);
        }
        return Status::OK();
    }
    if constexpr (PType == TYPE_DATEV2) {
        vectorized::CastParameters parameters;
        if (!vectorized::CastToDateV2::from_string({str.data(), str.size()}, res, nullptr,
                                                   parameters)) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "invalid {} string. str={}", type_to_string(data_type->get_primitive_type()),
                    str);
        }
        return Status::OK();
    }
    if constexpr (PType == TYPE_DATETIMEV2) {
        vectorized::CastParameters parameters;
        if (!vectorized::CastToDatetimeV2::from_string({str.data(), str.size()}, res, nullptr,
                                                       data_type->get_scale(), parameters)) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "invalid {} string. str={}", type_to_string(data_type->get_primitive_type()),
                    str);
        }
        return Status::OK();
    }
    if constexpr (PType == TYPE_TIMESTAMPTZ) {
        vectorized::CastParameters parameters;
        if (!vectorized::CastToTimstampTz::from_string({str.data(), str.size()}, res, parameters,
                                                       nullptr, data_type->get_scale())) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "invalid {} string. str={}", type_to_string(data_type->get_primitive_type()),
                    str);
        }
        return Status::OK();
    }
    if constexpr (PType == TYPE_CHAR) {
        size_t target = assert_cast<const vectorized::DataTypeString*>(
                                vectorized::remove_nullable(data_type).get())
                                ->len();
        res = {str.data(), str.size()};
        if (target > str.size()) {
            char* buffer = arena.alloc(target);
            memset(buffer, 0, target);
            memcpy(buffer, str.data(), str.size());
            res = {buffer, target};
        }
        return Status::OK();
    }
    if constexpr (PType == TYPE_STRING || PType == TYPE_VARCHAR) {
        char* buffer = arena.alloc(str.size());
        memcpy(buffer, str.data(), str.size());
        res = {buffer, str.size()};
        return Status::OK();
    }
    if constexpr (PType == TYPE_BOOLEAN) {
        vectorized::CastParameters parameters;
        vectorized::UInt8 tmp;
        if (!vectorized::CastToBool::from_string({str.data(), str.size()}, tmp, parameters)) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "invalid {} string. str={}", type_to_string(data_type->get_primitive_type()),
                    str);
        }
        res = tmp != 0;
        return Status::OK();
    }
    if constexpr (PType == TYPE_IPV4) {
        vectorized::CastParameters parameters;
        if (!vectorized::CastToIPv4::from_string({str.data(), str.size()}, res, parameters)) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "invalid {} string. str={}", type_to_string(data_type->get_primitive_type()),
                    str);
        }
        return Status::OK();
    }
    if constexpr (PType == TYPE_IPV6) {
        vectorized::CastParameters parameters;
        if (!vectorized::CastToIPv6::from_string({str.data(), str.size()}, res, parameters)) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "invalid {} string. str={}", type_to_string(data_type->get_primitive_type()),
                    str);
        }
        return Status::OK();
    }
    if constexpr (PType == TYPE_DECIMALV2) {
        vectorized::CastParameters parameters;
        vectorized::Decimal128V2 tmp;
        if (!vectorized::CastToDecimal::from_string({str.data(), str.size()}, tmp,
                                                    data_type->get_precision(),
                                                    data_type->get_scale(), parameters)) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "invalid {} string. str={}", type_to_string(data_type->get_primitive_type()),
                    str);
        }
        res = DecimalV2Value(tmp.value);
        return Status::OK();
    } else if constexpr (is_decimal(PType)) {
        vectorized::CastParameters parameters;
        if (!vectorized::CastToDecimal::from_string({str.data(), str.size()}, res,
                                                    data_type->get_precision(),
                                                    data_type->get_scale(), parameters)) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "invalid {} string. str={}", type_to_string(data_type->get_primitive_type()),
                    str);
        }
        return Status::OK();
    }
    return Status::Error<ErrorCode::INVALID_ARGUMENT>(
            "unsupported data type in delete handler. type={}",
            type_to_string(data_type->get_primitive_type()));
}

#define CONVERT_CASE(PType)                                            \
    case PType: {                                                      \
        set = build_set<PType>();                                      \
        for (const auto& s : str) {                                    \
            typename PrimitiveTypeTraits<PType>::CppType tmp;          \
            RETURN_IF_ERROR(convert<PType>(data_type, s, arena, tmp)); \
            set->insert(reinterpret_cast<const void*>(&tmp));          \
        }                                                              \
        return Status::OK();                                           \
    }
Status convert(const vectorized::DataTypePtr& data_type, const std::list<std::string>& str,
               vectorized::Arena& arena, std::shared_ptr<HybridSetBase>& set) {
    switch (data_type->get_primitive_type()) {
        CONVERT_CASE(TYPE_TINYINT);
        CONVERT_CASE(TYPE_SMALLINT);
        CONVERT_CASE(TYPE_INT);
        CONVERT_CASE(TYPE_BIGINT);
        CONVERT_CASE(TYPE_LARGEINT);
        CONVERT_CASE(TYPE_FLOAT);
        CONVERT_CASE(TYPE_DOUBLE);
        CONVERT_CASE(TYPE_DATE);
        CONVERT_CASE(TYPE_DATETIME);
        CONVERT_CASE(TYPE_DATEV2);
        CONVERT_CASE(TYPE_DATETIMEV2);
        CONVERT_CASE(TYPE_TIMESTAMPTZ);
        CONVERT_CASE(TYPE_BOOLEAN);
        CONVERT_CASE(TYPE_IPV4);
        CONVERT_CASE(TYPE_IPV6);
        CONVERT_CASE(TYPE_DECIMALV2);
        CONVERT_CASE(TYPE_DECIMAL32);
        CONVERT_CASE(TYPE_DECIMAL64);
        CONVERT_CASE(TYPE_DECIMAL128I);
        CONVERT_CASE(TYPE_DECIMAL256);
        CONVERT_CASE(TYPE_CHAR);
        CONVERT_CASE(TYPE_VARCHAR);
        CONVERT_CASE(TYPE_STRING);
    default:
        return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                "unsupported data type in delete handler. type={}",
                type_to_string(data_type->get_primitive_type()));
    }
    return Status::OK();
}
#undef CONVERT_CASE

#define CONVERT_CASE(PType)                                                                       \
    case PType: {                                                                                 \
        typename PrimitiveTypeTraits<PType>::CppType tmp;                                         \
        RETURN_IF_ERROR(convert<PType>(type, res.value_str.front(), arena, tmp));                 \
        v = vectorized::Field::create_field<PType>(tmp);                                          \
        switch (res.condition_op) {                                                               \
        case PredicateType::EQ:                                                                   \
            predicate = create_comparison_predicate<PredicateType::EQ>(index, col_name, type, v,  \
                                                                       true);                     \
            return Status::OK();                                                                  \
        case PredicateType::NE:                                                                   \
            predicate = create_comparison_predicate<PredicateType::NE>(index, col_name, type, v,  \
                                                                       true);                     \
            return Status::OK();                                                                  \
        case PredicateType::GT:                                                                   \
            predicate = create_comparison_predicate<PredicateType::GT>(index, col_name, type, v,  \
                                                                       true);                     \
            return Status::OK();                                                                  \
        case PredicateType::GE:                                                                   \
            predicate = create_comparison_predicate<PredicateType::GE>(index, col_name, type, v,  \
                                                                       true);                     \
            return Status::OK();                                                                  \
        case PredicateType::LT:                                                                   \
            predicate = create_comparison_predicate<PredicateType::LT>(index, col_name, type, v,  \
                                                                       true);                     \
            return Status::OK();                                                                  \
        case PredicateType::LE:                                                                   \
            predicate = create_comparison_predicate<PredicateType::LE>(index, col_name, type, v,  \
                                                                       true);                     \
            return Status::OK();                                                                  \
        default:                                                                                  \
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(                                    \
                    "invalid condition operator. operator={}", type_to_op_str(res.condition_op)); \
        }                                                                                         \
    }
Status parse_to_predicate(const uint32_t index, const std::string col_name,
                          const vectorized::DataTypePtr& type,
                          DeleteHandler::ConditionParseResult& res, vectorized::Arena& arena,
                          std::shared_ptr<ColumnPredicate>& predicate) {
    DCHECK_EQ(res.value_str.size(), 1);
    if (res.condition_op == PredicateType::IS_NULL ||
        res.condition_op == PredicateType::IS_NOT_NULL) {
        predicate = NullPredicate::create_shared(index, col_name,
                                                 res.condition_op == PredicateType::IS_NOT_NULL,
                                                 type->get_primitive_type());
        return Status::OK();
    }
    vectorized::Field v;
    switch (type->get_primitive_type()) {
        CONVERT_CASE(TYPE_TINYINT);
        CONVERT_CASE(TYPE_SMALLINT);
        CONVERT_CASE(TYPE_INT);
        CONVERT_CASE(TYPE_BIGINT);
        CONVERT_CASE(TYPE_LARGEINT);
        CONVERT_CASE(TYPE_FLOAT);
        CONVERT_CASE(TYPE_DOUBLE);
        CONVERT_CASE(TYPE_DATE);
        CONVERT_CASE(TYPE_DATETIME);
        CONVERT_CASE(TYPE_DATEV2);
        CONVERT_CASE(TYPE_DATETIMEV2);
        CONVERT_CASE(TYPE_TIMESTAMPTZ);
        CONVERT_CASE(TYPE_BOOLEAN);
        CONVERT_CASE(TYPE_IPV4);
        CONVERT_CASE(TYPE_IPV6);
        CONVERT_CASE(TYPE_DECIMALV2);
        CONVERT_CASE(TYPE_DECIMAL32);
        CONVERT_CASE(TYPE_DECIMAL64);
        CONVERT_CASE(TYPE_DECIMAL128I);
        CONVERT_CASE(TYPE_DECIMAL256);
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        v = vectorized::Field::create_field<TYPE_STRING>(res.value_str.front());
        switch (res.condition_op) {
        case PredicateType::EQ:
            predicate =
                    create_comparison_predicate<PredicateType::EQ>(index, col_name, type, v, true);
            return Status::OK();
        case PredicateType::NE:
            predicate =
                    create_comparison_predicate<PredicateType::NE>(index, col_name, type, v, true);
            return Status::OK();
        case PredicateType::GT:
            predicate =
                    create_comparison_predicate<PredicateType::GT>(index, col_name, type, v, true);
            return Status::OK();
        case PredicateType::GE:
            predicate =
                    create_comparison_predicate<PredicateType::GE>(index, col_name, type, v, true);
            return Status::OK();
        case PredicateType::LT:
            predicate =
                    create_comparison_predicate<PredicateType::LT>(index, col_name, type, v, true);
            return Status::OK();
        case PredicateType::LE:
            predicate =
                    create_comparison_predicate<PredicateType::LE>(index, col_name, type, v, true);
            return Status::OK();
        default:
            return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                    "invalid condition operator. operator={}", type_to_op_str(res.condition_op));
        }
        break;
    }
    default:
        return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                "unsupported data type in delete handler. type={}",
                type_to_string(type->get_primitive_type()));
    }
    return Status::OK();
#undef CONVERT_CASE
}

Status parse_to_in_predicate(const uint32_t index, const std::string& col_name,
                             const vectorized::DataTypePtr& type,
                             DeleteHandler::ConditionParseResult& res, vectorized::Arena& arena,
                             std::shared_ptr<ColumnPredicate>& predicate) {
    DCHECK_GT(res.value_str.size(), 1);
    switch (res.condition_op) {
    case PredicateType::IN_LIST: {
        std::shared_ptr<HybridSetBase> set;
        RETURN_IF_ERROR(convert(type, res.value_str, arena, set));
        predicate =
                create_in_list_predicate<PredicateType::IN_LIST>(index, col_name, type, set, true);
        break;
    }
    case PredicateType::NOT_IN_LIST: {
        std::shared_ptr<HybridSetBase> set;
        RETURN_IF_ERROR(convert(type, res.value_str, arena, set));
        predicate = create_in_list_predicate<PredicateType::NOT_IN_LIST>(index, col_name, type, set,
                                                                         true);
        break;
    }
    default:
        return Status::Error<ErrorCode::INVALID_ARGUMENT>("invalid condition operator. operator={}",
                                                          type_to_op_str(res.condition_op));
    }
    return Status::OK();
}

// construct sub condition from TCondition
std::string construct_sub_predicate(const TCondition& condition) {
    string op = condition.condition_op;
    if (op == "<") {
        op += "<";
    } else if (op == ">") {
        op += ">";
    }
    string condition_str;
    if ("IS" == op) {
        // ATTN: tricky! Surround IS with spaces to make it "special"
        condition_str = condition.column_name + " IS " + condition.condition_values[0];
    } else { // multi-elements IN expr has been processed with InPredicatePB
        if (op == "*=") {
            op = "=";
        } else if (op == "!*=") {
            op = "!=";
        }
        condition_str = condition.column_name + op + "'" + condition.condition_values[0] + "'";
    }
    return condition_str;
}

// make operators from FE adaptive to BE
std::string trans_op(const std::string& opt) {
    std::string op = string(opt);
    if (op == "<") {
        op += "<";
    } else if (op == ">") {
        op += ">";
    }
    if ("IS" != op) {
        if (op == "*=") {
            op = "=";
        } else if (op == "!*=") {
            op = "!=";
        }
    }
    return op;
}

Status DeleteHandler::generate_delete_predicate(const TabletSchema& schema,
                                                const std::vector<TCondition>& conditions,
                                                DeletePredicatePB* del_pred) {
    DBUG_EXECUTE_IF("DeleteHandler::generate_delete_predicate.inject_failure", {
        return Status::Error<false>(dp->param<int>("error_code"),
                                    dp->param<std::string>("error_msg"));
    })
    if (conditions.empty()) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                "invalid parameters for store_cond. condition_size={}", conditions.size());
    }

    // Check whether the delete condition meets the requirements
    for (const TCondition& condition : conditions) {
        RETURN_IF_ERROR(check_condition_valid(schema, condition));
    }

    // Store delete condition
    for (const TCondition& condition : conditions) {
        if (condition.condition_values.size() > 1) {
            InPredicatePB* in_pred = del_pred->add_in_predicates();
            if (condition.__isset.column_unique_id) {
                in_pred->set_column_unique_id(condition.column_unique_id);
            }
            in_pred->set_column_name(condition.column_name);
            bool is_not_in = condition.condition_op == "!*=";
            in_pred->set_is_not_in(is_not_in);
            for (const auto& condition_value : condition.condition_values) {
                in_pred->add_values(condition_value);
            }

            LOG(INFO) << "store one sub-delete condition. condition name=" << in_pred->column_name()
                      << "condition size=" << in_pred->values().size();
        } else {
            // write sub predicate v1 for compactbility
            std::string condition_str = construct_sub_predicate(condition);
            VLOG_NOTICE << __PRETTY_FUNCTION__ << " condition_str: " << condition_str;
            del_pred->add_sub_predicates(condition_str);
            DeleteSubPredicatePB* sub_predicate = del_pred->add_sub_predicates_v2();
            if (condition.__isset.column_unique_id) {
                // only light schema change capable table set this field
                sub_predicate->set_column_unique_id(condition.column_unique_id);
            } else {
                try {
                    [[maybe_unused]] auto parsed_cond = parse_condition(condition_str);
                } catch (const Exception& e) {
                    return Status::Error<ErrorCode::INVALID_ARGUMENT>(
                            "failed to parse condition_str, condition={}, error={}",
                            ThriftDebugString(condition), e.to_string());
                }
            }

            sub_predicate->set_column_name(condition.column_name);
            sub_predicate->set_op(trans_op(condition.condition_op));
            sub_predicate->set_cond_value(condition.condition_values[0]);
            LOG(INFO) << "store one sub-delete condition. condition="
                      << fmt::format(" {} {} {}", condition.column_name, condition.condition_op,
                                     condition.condition_values[0]);
        }
    }
    del_pred->set_version(-1);

    return Status::OK();
}

Status DeleteHandler::convert_to_sub_pred_v2(DeletePredicatePB* delete_pred,
                                             TabletSchemaSPtr schema) {
    if (!delete_pred->sub_predicates().empty() && delete_pred->sub_predicates_v2().empty()) {
        for (const auto& condition_str : delete_pred->sub_predicates()) {
            auto* sub_pred = delete_pred->add_sub_predicates_v2();
            auto condition = parse_condition(condition_str);
            const auto& column = *DORIS_TRY(schema->column(condition.column_name));
            sub_pred->set_column_unique_id(column.unique_id());
            sub_pred->set_column_name(condition.column_name);
            sub_pred->set_op(type_to_op_str(condition.condition_op));
            sub_pred->set_cond_value(condition.value_str.front());
        }
    }

    auto* in_pred_list = delete_pred->mutable_in_predicates();
    for (auto& in_pred : *in_pred_list) {
        const auto& column = *DORIS_TRY(schema->column(in_pred.column_name()));
        in_pred.set_column_unique_id(column.unique_id());
    }
    return Status::OK();
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
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
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
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ:
        return valid_datetime(value_str, column.frac());
    case FieldType::OLAP_FIELD_TYPE_BOOL:
        return valid_bool(value_str);
    case FieldType::OLAP_FIELD_TYPE_IPV4:
        return valid_ipv4(value_str);
    case FieldType::OLAP_FIELD_TYPE_IPV6:
        return valid_ipv6(value_str);
    default:
        LOG(WARNING) << "unknown field type. [type=" << int(field_type) << "]";
    }
    return false;
}

Status DeleteHandler::check_condition_valid(const TabletSchema& schema, const TCondition& cond) {
    // Check whether the column exists
    int32_t field_index = schema.field_index(cond.column_name);
    if (field_index < 0) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT>("field is not existent. [field_index={}]",
                                                          field_index);
    }

    // Delete condition should only applied on key columns or duplicate key table, and
    // the condition column type should not be float or double.
    const TabletColumn& column = schema.column(field_index);

    if (column.type() == FieldType::OLAP_FIELD_TYPE_DOUBLE ||
        column.type() == FieldType::OLAP_FIELD_TYPE_FLOAT) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT>("data type is float or double.");
    }

    // Check operator and operands size are matched.
    if ("*=" != cond.condition_op && "!*=" != cond.condition_op &&
        cond.condition_values.size() != 1) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT>("invalid condition value size. [size={}]",
                                                          cond.condition_values.size());
    }

    // Check each operand is valid
    for (const auto& condition_value : cond.condition_values) {
        if (!is_condition_value_valid(column, cond.condition_op, condition_value)) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT>("invalid condition value. [value={}]",
                                                              condition_value);
        }
    }

    if (!cond.__isset.column_unique_id) {
        LOG(WARNING) << "column=" << cond.column_name
                     << " in predicate does not have uid, table id=" << schema.table_id();
        // TODO(tsy): make it fail here after FE forbidding hard-link-schema-change
        return Status::OK();
    }
    if (schema.field_index(cond.column_unique_id) == -1) {
        const auto& err_msg =
                fmt::format("column id does not exists in table={}, schema version={},",
                            schema.table_id(), schema.schema_version());
        return Status::Error<ErrorCode::INVALID_ARGUMENT>(err_msg);
    }
    if (!iequal(schema.column_by_uid(cond.column_unique_id).name(), cond.column_name)) {
        const auto& err_msg = fmt::format(
                "colum name={} does not belongs to column uid={}, which "
                "column name={}, "
                "delete_cond.column_name ={}",
                cond.column_name, cond.column_unique_id,
                schema.column_by_uid(cond.column_unique_id).name(), cond.column_name);
        return Status::Error<ErrorCode::INVALID_ARGUMENT>(err_msg);
    }

    return Status::OK();
}

PredicateType DeleteHandler::parse_condition_op(const std::string& op_str,
                                                const std::list<std::string>& cond_values) {
    if (trim(to_lower(op_str)) == "=") {
        return PredicateType::EQ;
    } else if (trim(to_lower(op_str)) == "!=") {
        return PredicateType::NE;
    } else if (trim(to_lower(op_str)) == ">>") {
        return PredicateType::GT;
    } else if (trim(to_lower(op_str)) == "<<") {
        return PredicateType::LT;
    } else if (trim(to_lower(op_str)) == ">=") {
        return PredicateType::GE;
    } else if (trim(to_lower(op_str)) == "<=") {
        return PredicateType::LE;
    } else if (trim(to_lower(op_str)) == "*=") {
        return cond_values.size() > 1 ? PredicateType::IN_LIST : PredicateType::EQ;
    } else if (trim(to_lower(op_str)) == "!*=") {
        return cond_values.size() > 1 ? PredicateType::NOT_IN_LIST : PredicateType::NE;
    } else if (trim(to_lower(op_str)) == "is") {
        return to_lower(cond_values.front()) == "null" ? PredicateType::IS_NULL
                                                       : PredicateType::IS_NOT_NULL;
    } else {
        throw Exception(Status::Error<ErrorCode::INVALID_ARGUMENT>(
                "invalid condition operator. operator={}", op_str));
    }
    return PredicateType::UNKNOWN;
}

DeleteHandler::ConditionParseResult DeleteHandler::parse_condition(
        const DeleteSubPredicatePB& sub_cond) {
    ConditionParseResult res;
    if (!sub_cond.has_column_name() || !sub_cond.has_op() || !sub_cond.has_cond_value()) {
        throw Exception(Status::Error<ErrorCode::INVALID_ARGUMENT>(
                "fail to parse condition. condition={} {} {}", sub_cond.column_name(),
                sub_cond.op(), sub_cond.cond_value()));
    }
    if (sub_cond.has_column_unique_id()) {
        res.col_unique_id = sub_cond.column_unique_id();
    }
    res.column_name = sub_cond.column_name();
    res.value_str.push_back(sub_cond.cond_value());
    res.condition_op = parse_condition_op(sub_cond.op(), res.value_str);
    return res;
}

// clang-format off
// Condition string format, the format is (column_name)(op)(value)
// eg: condition_str="c1 = 1597751948193618247 and length(source)<1;\n;\n"
// column_name: matches "c1", must include FeNameFormat.java COLUMN_NAME_REGEX
//              and compactible with any the lagacy
// operator: matches "="
// value: matches "1597751948193618247  and length(source)<1;\n;\n"
//
// For more info, see DeleteHandler::construct_sub_predicates
// FIXME(gavin): This is a tricky implementation, it should not be the final resolution, refactor it.
const char* const CONDITION_STR_PATTERN =
    // .----------------- column-name --------------------------.   .----------------------- operator ------------------------.   .------------ value ----------.
    R"(([_a-zA-Z@0-9\s/\p{L}][.a-zA-Z0-9_+-/?@#$%^&*"\s,:\p{L}]*)\s*((?:=)|(?:!=)|(?:>>)|(?:<<)|(?:>=)|(?:<=)|(?:\*=)|(?: IS ))\s*('((?:[\s\S]+)?)'|(?:[\s\S]+)?))";
    // '----------------- group 1 ------------------------------'   '--------------------- group 2 ---------------------------'   | '-- group 4--'              |
    //                                                                   match any of: = != >> << >= <= *= " IS "                 '----------- group 3 ---------'
    //                                                                                                                             match **ANY THING** without(4)
    //                                                                                                                             or with(3) single quote
// clang-format on
RE2 DELETE_HANDLER_REGEX(CONDITION_STR_PATTERN);

DeleteHandler::ConditionParseResult DeleteHandler::parse_condition(
        const std::string& condition_str) {
    ConditionParseResult res;
    std::string col_name, op, value, g4;

    bool matched = RE2::FullMatch(condition_str, DELETE_HANDLER_REGEX, &col_name, &op, &value,
                                  &g4); // exact match

    if (!matched) {
        throw Exception(
                Status::InvalidArgument("fail to sub condition. condition={}", condition_str));
    }

    res.column_name = col_name;

    // match string with single quotes, a = b  or a = 'b'
    if (!g4.empty()) {
        res.value_str.push_back(g4);
    } else {
        res.value_str.push_back(value);
    }
    res.condition_op = DeleteHandler::parse_condition_op(op, res.value_str);
    VLOG_NOTICE << "parsed condition_str: col_name={" << col_name << "} op={" << op << "} val={"
                << res.value_str.back() << "}";
    return res;
}

template <typename SubPredType>
    requires(std::is_same_v<SubPredType, DeleteSubPredicatePB> or
             std::is_same_v<SubPredType, std::string>)
Status DeleteHandler::_parse_column_pred(TabletSchemaSPtr complete_schema,
                                         TabletSchemaSPtr delete_pred_related_schema,
                                         const RepeatedPtrField<SubPredType>& sub_pred_list,
                                         DeleteConditions* delete_conditions) {
    for (const auto& sub_predicate : sub_pred_list) {
        auto condition = parse_condition(sub_predicate);
        int32_t col_unique_id = -1;
        if constexpr (std::is_same_v<SubPredType, DeleteSubPredicatePB>) {
            if (sub_predicate.has_column_unique_id()) [[likely]] {
                col_unique_id = sub_predicate.column_unique_id();
            }
        }
        if (col_unique_id < 0) {
            const auto& column =
                    *DORIS_TRY(delete_pred_related_schema->column(condition.column_name));
            col_unique_id = column.unique_id();
        }
        condition.col_unique_id = col_unique_id;
        const auto& column = complete_schema->column_by_uid(col_unique_id);
        uint32_t index = complete_schema->field_index(col_unique_id);
        std::shared_ptr<ColumnPredicate> predicate;
        RETURN_IF_ERROR(parse_to_predicate(index, column.name(), column.get_vec_type(), condition,
                                           _predicate_arena, predicate));
        if (predicate != nullptr) {
            delete_conditions->column_predicate_vec.push_back(predicate);
        }
    }
    return Status::OK();
}

Status DeleteHandler::init(TabletSchemaSPtr tablet_schema,
                           const std::vector<RowsetMetaSharedPtr>& delete_preds, int64_t version) {
    DCHECK(!_is_inited) << "reinitialize delete handler.";
    DCHECK(version >= 0) << "invalid parameters. version=" << version;

    for (const auto& delete_pred : delete_preds) {
        // Skip the delete condition with large version
        if (delete_pred->version().first > version) {
            continue;
        }
        // Need the tablet schema at the delete condition to parse the accurate column
        const auto& delete_pred_related_schema = delete_pred->tablet_schema();
        const auto& delete_condition = delete_pred->delete_predicate();
        DeleteConditions temp;
        temp.filter_version = delete_pred->version().first;
        if (!delete_condition.sub_predicates_v2().empty()) {
            RETURN_IF_ERROR(_parse_column_pred(tablet_schema, delete_pred_related_schema,
                                               delete_condition.sub_predicates_v2(), &temp));
        } else {
            // make it compatible with the former versions
            RETURN_IF_ERROR(_parse_column_pred(tablet_schema, delete_pred_related_schema,
                                               delete_condition.sub_predicates(), &temp));
        }
        for (const auto& in_predicate : delete_condition.in_predicates()) {
            ConditionParseResult condition;
            condition.column_name = in_predicate.column_name();

            int32_t col_unique_id = -1;
            if (in_predicate.has_column_unique_id()) {
                col_unique_id = in_predicate.column_unique_id();
            } else {
                // if upgrade from version 2.0.x, column_unique_id maybe not set
                const auto& pre_column =
                        *DORIS_TRY(delete_pred_related_schema->column(condition.column_name));
                col_unique_id = pre_column.unique_id();
            }
            if (col_unique_id == -1) {
                return Status::Error<ErrorCode::DELETE_INVALID_CONDITION>(
                        "cannot get column_unique_id for column {}", condition.column_name);
            }
            condition.col_unique_id = col_unique_id;

            condition.condition_op =
                    in_predicate.is_not_in() ? PredicateType::NOT_IN_LIST : PredicateType::IN_LIST;
            for (const auto& value : in_predicate.values()) {
                condition.value_str.push_back(value);
            }
            const auto& column = tablet_schema->column_by_uid(col_unique_id);
            uint32_t index = tablet_schema->field_index(col_unique_id);
            std::shared_ptr<ColumnPredicate> predicate;
            RETURN_IF_ERROR(parse_to_in_predicate(index, column.name(), column.get_vec_type(),
                                                  condition, _predicate_arena, predicate));
            temp.column_predicate_vec.push_back(predicate);
        }

        _del_conds.emplace_back(std::move(temp));
    }

    _is_inited = true;

    return Status::OK();
}

DeleteHandler::~DeleteHandler() {
    if (!_is_inited) {
        return;
    }

    _del_conds.clear();
    _is_inited = false;
}

void DeleteHandler::get_delete_conditions_after_version(
        int64_t version, AndBlockColumnPredicate* and_block_column_predicate_ptr,
        std::unordered_map<int32_t, std::vector<std::shared_ptr<const ColumnPredicate>>>*
                del_predicates_for_zone_map) const {
    for (const auto& del_cond : _del_conds) {
        if (del_cond.filter_version > version) {
            // now, only query support delete column predicate operator
            if (!del_cond.column_predicate_vec.empty()) {
                if (del_cond.column_predicate_vec.size() == 1) {
                    auto single_column_block_predicate = SingleColumnBlockPredicate::create_unique(
                            del_cond.column_predicate_vec[0]);
                    and_block_column_predicate_ptr->add_column_predicate(
                            std::move(single_column_block_predicate));
                    if (del_predicates_for_zone_map->count(
                                del_cond.column_predicate_vec[0]->column_id()) < 1) {
                        del_predicates_for_zone_map->insert(
                                {del_cond.column_predicate_vec[0]->column_id(),
                                 std::vector<std::shared_ptr<const ColumnPredicate>> {}});
                    }
                    (*del_predicates_for_zone_map)[del_cond.column_predicate_vec[0]->column_id()]
                            .push_back(del_cond.column_predicate_vec[0]);
                } else {
                    auto or_column_predicate = OrBlockColumnPredicate::create_unique();

                    // build or_column_predicate
                    // when delete from where a = 1 and b = 2, we can not use del_predicates_for_zone_map to filter zone page,
                    // so here do not put predicate to del_predicates_for_zone_map,
                    // refer #17145 for more details.
                    // // TODO: need refactor design and code to use more version delete and more column delete to filter zone page.
                    std::for_each(del_cond.column_predicate_vec.cbegin(),
                                  del_cond.column_predicate_vec.cend(),
                                  [&or_column_predicate](
                                          const std::shared_ptr<const ColumnPredicate> predicate) {
                                      or_column_predicate->add_column_predicate(
                                              SingleColumnBlockPredicate::create_unique(predicate));
                                  });
                    and_block_column_predicate_ptr->add_column_predicate(
                            std::move(or_column_predicate));
                }
            }
        }
    }
}

} // namespace doris
