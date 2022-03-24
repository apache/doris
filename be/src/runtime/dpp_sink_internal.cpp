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

#include "runtime/dpp_sink_internal.h"

#include <thrift/protocol/TDebugProtocol.h>

#include "common/object_pool.h"
#include "exec/text_converter.hpp"
#include "exprs/expr.h"
#include "gen_cpp/DataSinks_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris {

PartRangeKey PartRangeKey::_s_pos_infinite(1);
PartRangeKey PartRangeKey::_s_neg_infinite(-1);

PartRange PartRange::_s_all_range(PartRangeKey::neg_infinite(), PartRangeKey::pos_infinite(), true, true);

Status PartRangeKey::from_thrift(ObjectPool* pool, const TPartitionKey& t_key, PartRangeKey* key) {
    key->_sign = t_key.sign;
    if (key->_sign != 0) {
        return Status::OK();
    }

    key->_type = thrift_to_type(t_key.type);
    key->_str_key = t_key.key;

    StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
    switch (key->_type) {
    case TYPE_TINYINT: {
        key->_key = pool->add(new int8_t());
        int8_t* int_value = reinterpret_cast<int8_t*>(key->_key);
        *int_value = StringParser::string_to_int<int8_t>(t_key.key.c_str(), t_key.key.length(),
                                                         &parse_result);
        break;
    }

    case TYPE_SMALLINT: {
        key->_key = pool->add(new int16_t());
        int16_t* int_value = reinterpret_cast<int16_t*>(key->_key);
        *int_value = StringParser::string_to_int<int16_t>(t_key.key.c_str(), t_key.key.length(),
                                                          &parse_result);
        break;
    }

    case TYPE_INT: {
        key->_key = pool->add(new int32_t());
        int32_t* int_value = reinterpret_cast<int32_t*>(key->_key);
        *int_value = StringParser::string_to_int<int32_t>(t_key.key.c_str(), t_key.key.length(),
                                                          &parse_result);
        break;
    }

    case TYPE_BIGINT: {
        key->_key = pool->add(new int64_t());
        int64_t* int_value = reinterpret_cast<int64_t*>(key->_key);
        *int_value = StringParser::string_to_int<int64_t>(t_key.key.c_str(), t_key.key.length(),
                                                          &parse_result);
        break;
    }

    case TYPE_LARGEINT: {
        key->_key = pool->add(new __int128());
        __int128* int_value = reinterpret_cast<__int128*>(key->_key);
        *int_value = StringParser::string_to_int<__int128>(t_key.key.c_str(), t_key.key.length(),
                                                           &parse_result);
        break;
    }

    case TYPE_DATE: {
        key->_key = pool->add(new DateTimeValue());
        DateTimeValue* datetime = reinterpret_cast<DateTimeValue*>(key->_key);
        if (!(datetime->from_date_str(t_key.key.c_str(), t_key.key.length()))) {
            std::stringstream error_msg;
            error_msg << "Fail to convert date string:" << t_key.key;
            return Status::InternalError(error_msg.str());
        }
        datetime->cast_to_date();
        break;
    }

    case TYPE_DATETIME: {
        key->_key = pool->add(new DateTimeValue());
        DateTimeValue* datetime = reinterpret_cast<DateTimeValue*>(key->_key);
        if (!(datetime->from_date_str(t_key.key.c_str(), t_key.key.length()))) {
            std::stringstream error_msg;
            error_msg << "Fail to convert datetime string:" << t_key.key;
            return Status::InternalError(error_msg.str());
        }
        datetime->to_datetime();
        break;
    }

    default:
        DCHECK(false) << "bad partition key column type: " << type_to_string(key->_type);
        break;
    }
    if (parse_result != StringParser::PARSE_SUCCESS) {
        std::stringstream error_msg;
        error_msg << "Fail to convert string:" << t_key.key;
        return Status::InternalError(error_msg.str());
    }

    return Status::OK();
}

Status PartRangeKey::from_value(PrimitiveType type, void* value, PartRangeKey* key) {
    key->_sign = 0;
    key->_type = type;
    key->_key = value;

    return Status::OK();
}

Status PartRange::from_thrift(ObjectPool* pool, const TPartitionRange& t_part_range,
                              PartRange* range) {
    VLOG_ROW << "construct from thrift: " << apache::thrift::ThriftDebugString(t_part_range);
    RETURN_IF_ERROR(PartRangeKey::from_thrift(pool, t_part_range.start_key, &range->_start_key));
    RETURN_IF_ERROR(PartRangeKey::from_thrift(pool, t_part_range.end_key, &range->_end_key));
    range->_include_start_key = t_part_range.include_start_key;
    range->_include_end_key = t_part_range.include_end_key;
    VLOG_ROW << "after construct: " << range->debug_string();
    return Status::OK();
}

Status PartitionInfo::from_thrift(ObjectPool* pool, const TRangePartition& t_partition,
                                  PartitionInfo* partition) {
    partition->_id = t_partition.partition_id;
    RETURN_IF_ERROR(PartRange::from_thrift(pool, t_partition.range, &partition->_range));
    if (t_partition.__isset.distributed_exprs) {
        partition->_distributed_bucket = t_partition.distribute_bucket;
        if (partition->_distributed_bucket == 0) {
            return Status::InternalError("Distributed bucket is 0.");
        }
        RETURN_IF_ERROR(Expr::create_expr_trees(pool, t_partition.distributed_exprs,
                                                &partition->_distributed_expr_ctxs));
    }
    return Status::OK();
}

Status PartitionInfo::prepare(RuntimeState* state, const RowDescriptor& row_desc,
                              const std::shared_ptr<MemTracker>& mem_tracker) {
    if (_distributed_expr_ctxs.size() > 0) {
        RETURN_IF_ERROR(Expr::prepare(_distributed_expr_ctxs, state, row_desc, mem_tracker));
    }
    return Status::OK();
}

Status PartitionInfo::open(RuntimeState* state) {
    if (_distributed_expr_ctxs.size() > 0) {
        return Expr::open(_distributed_expr_ctxs, state);
    }
    return Status::OK();
}

void PartitionInfo::close(RuntimeState* state) {
    if (_distributed_expr_ctxs.size() > 0) {
        Expr::close(_distributed_expr_ctxs, state);
    }
}

} // namespace doris
